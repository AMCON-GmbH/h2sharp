#region MIT License
/*
 * Copyright � 2008 Jonathan Mark Porter.
 * H2Sharp is a wrapper for the H2 Database Engine. http://h2sharp.googlecode.com
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following
 * conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */
#endregion

using java.sql;
using org.h2.jdbc;
using System.Collections.Generic;
using System.Text;
using DbCommand = System.Data.Common.DbCommand;
using DbConnection = System.Data.Common.DbConnection;
using DbDataReader = System.Data.Common.DbDataReader;
using DbParameter = System.Data.Common.DbParameter;
using DbParameterCollection = System.Data.Common.DbParameterCollection;
using DbTransaction = System.Data.Common.DbTransaction;

namespace System.Data.H2;

public sealed class H2Command : DbCommand {
    #region sub classes
    private class PreparedTemplate {
        public PreparedTemplate(string oldSql, string trueSql, int[] mapping, int paramCount) {
            this.OldSql = oldSql;
            this.TrueSql = trueSql;
            this.Mapping = mapping;
            this.paramCount = paramCount;
        }

        public int paramCount { get; }

        public string OldSql { get; }

        public string TrueSql { get; }

        public int[] Mapping { get; }
    }
    #endregion

    #region static
    private static readonly Dictionary<string, PreparedTemplate> templates = new();
    private static readonly object syncRoot = new();

    private static int[] CreateRange(int length) {
        int[] result = new int[length];

        for (int index = 0; index < length; ++index) {
            result[index] = index;
        }

        return result;
    }
    #endregion

    #region fields
    private int commandTimeout = 30;
    private bool timeoutSet;
    private PreparedStatement statement;
    private PreparedTemplate template;
    #endregion

    #region constructors
    public H2Command()
        : this(null, null, null) { }

    public H2Command(H2Connection connection)
        : this(null, connection, null) { }

    public H2Command(string commandText)
        : this(commandText, null, null) { }

    public H2Command(string commandText, H2Connection connection)
        : this(commandText, connection, null) { }

    public H2Command(string commandText, H2Connection connection, H2Transaction transaction) {
        this.CommandText = commandText;
        this.Connection = connection;
        Parameters = new H2ParameterCollection();
        UpdatedRowSource = UpdateRowSource.None;
    }
    #endregion

    #region properties
    public new H2Connection Connection { get; set; }

    public new H2ParameterCollection Parameters { get; }

    public new H2Transaction Transaction {
        get {
            if (Connection == null) { return null; }

            return Connection.transaction;
        }
        set {
            if (value == null)
                return; // { throw new ArgumentNullException("value"); }

            Connection = value.Connection;
        }
    }

    protected override DbConnection DbConnection {
        get => Connection;
        set => Connection = (H2Connection) value;
    }

    protected override DbParameterCollection DbParameterCollection => Parameters;

    protected override DbTransaction DbTransaction {
        get => Transaction;
        set => Transaction = (H2Transaction) value;
    }

    public override string CommandText { get; set; }

    public override int CommandTimeout {
        get => commandTimeout;
        set {
            timeoutSet = true;
            commandTimeout = value;
        }
    }

    public override CommandType CommandType { get; set; }

    public override bool DesignTimeVisible { get; set; }

    public override UpdateRowSource UpdatedRowSource { get; set; }

    /// <summary>
    ///     This is here if you are having problems with Named Parameters. it turns them off.
    ///     if you have to set this to true inform me at (http://groups.google.com/group/H2Sharp)
    /// </summary>
    public bool DisableNamedParameters { get; set; }

    private bool IsNamed {
        get {
            if (DisableNamedParameters) { return false; }

            bool inQuote = false;

            for (int index = 0; index < CommandText.Length; ++index) {
                char c = CommandText[index];

                if (!inQuote && c == ':') {
                    return true;
                }

                if (c == '\'') {
                    inQuote = !inQuote;
                }
            }

            return false;
        }
    }
    #endregion

    #region methods
    private void CheckConnection() {
        if (Connection == null) { throw new H2Exception("DbConnection must be set."); }

        Connection.CheckIsOpen();
    }

    private PreparedTemplate CreateNameTemplate() {
        var list = new List<int>();
        var command = new StringBuilder();
        var name = new StringBuilder();
        bool inQuote = false;
        bool blockComment = false;
        bool lineComment = false;
        CommandText += " ";
        int index = 0;
        int paramCount = 0;
        int lastParamCount = 0;

        while (index < CommandText.Length) {
            char c = CommandText[index];
            char c1 = index + 1 < CommandText.Length ? CommandText[index + 1] : ' ';
            index++;

            if (name.Length == 0) {
                if (!inQuote && !blockComment && !lineComment && c == ':') {
                    name.Append(c);
                } else if (!inQuote && string.Concat(c, c1) == "/*") {
                    blockComment = true;
                    index++;
                } else if (!inQuote && string.Concat(c, c1) == "*/") {
                    blockComment = false;
                    index++;
                } else if (!inQuote && !blockComment && string.Concat(c, c1) == "--") {
                    lineComment = true;
                    index++;
                } else if (lineComment && string.Concat(c, c1) == "\r\n") {
                    lineComment = false;
                    index++;
                } else {
                    if (c == '\'' && !blockComment && !lineComment) {
                        inQuote = !inQuote;
                    }

                    if (!blockComment && !lineComment) command.Append(c);

                    if (!inQuote && !blockComment && !lineComment && c == ';') {
                        lastParamCount = paramCount;
                        paramCount = 0;
                    }
                }
            } else {
                if (char.IsLetterOrDigit(c) || c == '_') {
                    name.Append(c);
                } else {
                    paramCount++;
                    command.Append('?');
                    command.Append(c);

                    if (c == ';') {
                        lastParamCount = paramCount;
                        paramCount = 0;
                    }

                    string paramName = name.ToString().Replace(":", "");
                    int paramIndex = Parameters.FindIndex(delegate(H2Parameter p) { return p.ParameterName == paramName; });

                    if (paramIndex == -1) {
                        paramName = name.ToString();
                        paramIndex = Parameters.FindIndex(delegate(H2Parameter p) { return p.ParameterName == paramName; });

                        if (paramIndex == -1) { throw new H2Exception(string.Format("Missing Parameter: {0}", paramName)); }
                    }

                    name.Length = 0;
                    list.Add(paramIndex);
                }
            }
        }

        CommandText = CommandText.Trim().Trim();
        return new PreparedTemplate(CommandText, command.ToString(), list.ToArray(), lastParamCount);
    }

    private PreparedTemplate CreateIndexTemplate() {
        int count = 0;
        int paramCount = 0;
        int lastParamCount = 0;
        int index = 0;
        bool inQuote = false;

        while (index < CommandText.Length) {
            char c = CommandText[index];
            index++;

            if (c == '\'') {
                inQuote = !inQuote;
            }

            if (c == '?' && !inQuote) {
                paramCount++;
                count++;
            }

            if (c == ';' && !inQuote) {
                lastParamCount = paramCount;
                paramCount = 0;
            }
        }

        return new PreparedTemplate(CommandText, CommandText, CreateRange(count), lastParamCount);
    }

    private void CreateStatement() {
        if (statement != null) {
            statement.close();
        }

        try {
            statement = Connection.connection.prepareStatement(template.TrueSql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
        } catch (JdbcSQLException ex) {
            throw new H2Exception(ex);
        }

        if (timeoutSet) {
            statement.setQueryTimeout(commandTimeout);
        }
    }

    protected void stripComments() {
        var command = new StringBuilder();
        bool inQuote = false;
        bool blockComment = false;
        bool lineComment = false;
        CommandText += " ";
        int index = 0;

        while (index < CommandText.Length) {
            char c = CommandText[index];
            char c1 = index + 1 < CommandText.Length ? CommandText[index + 1] : ' ';
            index++;

            if (!inQuote && !lineComment && string.Concat(c, c1) == "/*") {
                blockComment = true;
                index++;
            } else if (!inQuote && string.Concat(c, c1) == "*/") {
                blockComment = false;
                index++;
                command.Append(' ');
            } else if (!inQuote && !blockComment && string.Concat(c, c1) == "--") {
                lineComment = true;
                index++;
            } else if (lineComment && string.Concat(c, c1) == "\r\n") {
                lineComment = false;
                index++;
                command.Append(' ');

            } else {
                if (c == '\'' && !blockComment && !lineComment) {
                    inQuote = !inQuote;
                }

                if (!blockComment && !lineComment) command.Append(c);
            }

        }

        CommandText = command.ToString().Trim().Trim();
    }

    private void EnsureStatment() {
        if (CommandText == null) { throw new InvalidOperationException("must set CommandText"); }

        stripComments();

        if (template == null || template.OldSql != CommandText) {
            lock (syncRoot) {
                if (!templates.TryGetValue(CommandText, out template)) {
                    if (IsNamed) {
                        template = CreateNameTemplate();
                    } else {
                        template = CreateIndexTemplate();
                    }

                    templates.Add(CommandText, template);
                }
            }

            CreateStatement();
        } else {
            statement.clearParameters();
        }

        for (int index = 0; index < template.Mapping.Length; ++index) {
            Parameters[template.Mapping[index]].SetStatement(index + 1, statement);
        }
    }

    protected override DbParameter CreateDbParameter() {
        return new H2Parameter();
    }

    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) {
        return ExecuteReader(behavior);
    }

    public new H2Parameter CreateParameter() {
        return new H2Parameter();
    }

    public new H2DataReader ExecuteReader() {
        return ExecuteReader(CommandBehavior.Default);
    }

    public new H2DataReader ExecuteReader(CommandBehavior behavior) {
        // TODO check this : if (behavior != CommandBehavior.Default) { throw new NotSupportedException("Only CommandBehavior Default is supported for now."); }
        CheckConnection();
        EnsureStatment();

        try {
            string low = CommandText.ToLower().Trim();
            int iSemi = low.IndexOf(';');

            if ((low.StartsWith("insert") || low.StartsWith("update")) && (iSemi < 0 || iSemi == low.Length - 1)) {
                statement.executeUpdate();
                return null;
            }

            return new H2DataReader(Connection, statement.executeQuery());
        } catch (JdbcSQLException ex) {
            ex.printStackTrace();
            throw new H2Exception(ex);
        }
    }

    public override void Cancel() {
        CheckConnection();

        if (statement != null) {
            try {
                statement.cancel();
            } catch (JdbcSQLException ex) {
                throw new H2Exception(ex);
            }
        }
    }

    public override int ExecuteNonQuery() {
        CheckConnection();
        EnsureStatment();

        try {
            int ret = statement.executeUpdate();

            if (ret == 1 && template.paramCount > 0 && template.Mapping.Length > template.paramCount)
                return (int) Math.Ceiling(template.Mapping.Length * 1.0 / template.paramCount);

            return ret;
        } catch (JdbcSQLException ex) {
            throw new H2Exception(ex);
        }
    }

    public override object ExecuteScalar() {
        CheckConnection();
        EnsureStatment();
        object result = null;
        if (CommandText?.Replace(" ", "").Replace("\r\n", "").Length == 0) return result;

        try {
            ResultSet set = statement.executeQuery();

            try {
                if (set.next()) {
                    result = set.getObject(1);

                    if (result == null)
                        result = DBNull.Value;
                    else
                        result = H2Helper.ConverterToCLR(set.getMetaData().getColumnType(1))(result);
                }
            } finally {
                set.close();
            }

            return result;
        } catch (JdbcSQLException ex) {
            throw new H2Exception(ex);
        }
    }

    public override void Prepare() {
        CheckConnection();
        EnsureStatment();
    }

    protected override void Dispose(bool disposing) {
        base.Dispose(disposing);

        if (disposing) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
        }
    }
    #endregion
}