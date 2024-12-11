using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text.RegularExpressions;

namespace System.Data.H2;

/// <summary>
///     This command builder is still buggy, please only use it to debug it :-)
/// </summary>
public class H2CommandBuilder : DbCommandBuilder {
    //H2Connection connection;
    private static readonly Regex selectRegex = new(
        "^select\\s+(.*)\\s+from\\s+([^\\s]+?)(?:\\s+where\\s+(?:.*))?(?:\\s+order\\s+by\\s+(?:.*))?$",
        RegexOptions.Multiline | RegexOptions.IgnoreCase | RegexOptions.Compiled
    );
    private static readonly Regex columnRegex = new("\"(.*)\"", RegexOptions.Compiled | RegexOptions.Multiline);

    public H2CommandBuilder(H2DataAdapter adapter) {
        DataAdapter = adapter;

        //Letting ADO.NET do its job does not appear to work (yet) :
        if (false) {
            adapter.InsertCommand = (H2Command) GetInsertCommand();
            adapter.UpdateCommand = (H2Command) GetUpdateCommand();
            return;
        }

        H2Connection? connection = adapter.SelectCommand.Connection;
        string? select = adapter.SelectCommand.CommandText.ToLower();
        Match? mat = selectRegex.Match(select);

        if (!mat.Success)
            throw new Exception("Select command not recognized : '" + select + "'");

        string? tableName = mat.Groups[2].Value;

        {
            Match? mmat = columnRegex.Match(tableName);

            if (mmat.Success)
                tableName = mmat.Groups[1].Value;
        }

        Dictionary<string, int>? columnTypeCodes = connection.GetColumnTypeCodes(tableName);

        IList<string> cols = mat.Groups[1].Value.Split(',');

        if (cols.Count == 1 && cols[0].Trim().Equals("*"))
            cols = columnTypeCodes.Keys.ToList();

        cols = cols.Select(c => c.Trim()).ToList();

        var updateCommand = new H2Command(connection);
        var insertCommand = new H2Command(connection);
        var updateSets = new List<string>();
        var updateWheres = new List<string>();
        //var namesUp = new List<String>();
        //var valuesUp = new List<String>();
        var colasrx = new Regex("\"?(.*)\"? as \"?(.*)\"?");
        int nextParam = 0;
        var aliases = new Dictionary<string, string>();

        foreach (string? col in cols) {
            Match? colasmat = colasrx.Match(col);
            string alias;
            string columnName;

            if (colasmat.Success) {
                alias = colasmat.Groups[2].Value.ToUpper().Trim();
                columnName = colasmat.Groups[1].Value.ToUpper().Trim();
            } else {
                alias = columnName = col.ToUpper().Trim();
            }

            aliases[columnName] = alias;
            string? paramName = (nextParam++).ToString();

            updateSets.Add("\"" + columnName + "\" = ?"); //:" + paramName);

            int typeCode = columnTypeCodes[columnName];
            DbType dbType = H2Helper.GetDbType(typeCode);

            updateCommand.Parameters.Add(
                new H2Parameter(paramName, dbType) {
                    SourceColumn = alias,
                    DbType = dbType,
                    Direction = ParameterDirection.Input,
                    SourceVersion = DataRowVersion.Current
                }
            );

        }

        HashSet<string>? pks = connection.GetPrimaryKeysColumns(tableName);

        foreach (string? pk in pks.Select(c => c.ToUpper())) {
            string? columnName = pk;
            string? paramName = (nextParam++).ToString();
            updateWheres.Add("\"" + columnName + "\" = ?"); //:" + paramName);

            string alias;

            if (!aliases.TryGetValue(columnName, out alias))
                alias = columnName;

            int typeCode = columnTypeCodes[columnName];
            DbType dbType = H2Helper.GetDbType(typeCode);

            updateCommand.Parameters.Add(
                new H2Parameter(paramName, dbType) {
                    SourceColumn = alias,
                    DbType = dbType,
                    Direction = ParameterDirection.Input,
                    SourceVersion = DataRowVersion.Original
                }
            );
        }

        var insertValues = new List<string>();
        nextParam = 0;

        foreach (string? columnName in cols.Select(c => c.ToUpper())) {
            string? paramName = (nextParam++).ToString();
            insertValues.Add("?"); //":" + paramName);
            string alias;

            if (!aliases.TryGetValue(columnName, out alias))
                alias = columnName;

            int typeCode = columnTypeCodes[columnName];
            DbType dbType = H2Helper.GetDbType(typeCode);

            insertCommand.Parameters.Add(
                new H2Parameter(paramName, dbType) {
                    SourceColumn = alias,
                    DbType = dbType,
                    Direction = ParameterDirection.Input,
                    SourceVersion = DataRowVersion.Original
                }
            );
        }

        updateCommand.CommandText = "update " + tableName + " set " + updateSets.Commas() + " where " + updateWheres.Commas();
        adapter.UpdateCommand = updateCommand;
        insertCommand.CommandText = "insert into " + tableName + "(" + cols.Commas() + ") values (" + insertValues.Commas() + ")";
        adapter.InsertCommand = insertCommand;

    }

    protected override void ApplyParameterInfo(DbParameter parameter, DataRow row, StatementType statementType, bool whereClause) {
        parameter.DbType = (DbType) row["DbType"];
    }

    protected override string GetParameterName(string parameterName) {
        return parameterName;
    }

    protected override string GetParameterName(int parameterOrdinal) {
        return "param" + parameterOrdinal;
    }

    protected override string GetParameterPlaceholder(int parameterOrdinal) {
        return "?";
    }

    protected override void SetRowUpdatingHandler(DbDataAdapter adapter) {
        //throw new NotImplementedException();
    }
}

public static class ConnectionExtensions {
    public static List<string> ReadStrings(this H2Connection connection, string query) {
        var ret = new List<string>();
        H2DataReader? reader = new H2Command(query, connection).ExecuteReader();

        while (reader.Read())
            ret.Add(reader.GetString(0));

        return ret;
    }

    public static DataTable ReadTable(this H2Connection connection, string tableName) {
        if (tableName == null)
            return null;

        return connection.ReadQuery("select * from \"" + tableName + "\"");
    }

    public static DataTable ReadQuery(this H2Connection connection, string query) {
        if (query == null)
            return null;

        var table = new DataTable {
            CaseSensitive = false
        };

        new H2DataAdapter(new H2Command(query, connection)).Fill(table);
        return table;
    }

    public static string ReadString(this H2Connection connection, string query) {
        string? result = new H2Command(query, connection).ExecuteScalar() as string;
        return result;
    }

    public static Dictionary<string, T> ReadMap<T>(this H2Connection connection, string query) {
        var ret = new Dictionary<string, T>();
        H2DataReader? reader = new H2Command(query, connection).ExecuteReader();

        while (reader.Read()) {
            string? key = reader.GetString(0);
            object? value = reader.GetValue(1);

            if (value == DBNull.Value)
                ret[key] = default;
            else
                ret[key] = (T) value;
        }

        return ret;
    }
}

public static class CollectionExtensions {
    public static T[] Array<T>(params T[] a) {
        return a;
    }

    public static string Commas<T>(this IEnumerable<T> col) {
        return col.Implode(", ");
    }

    public static string Implode<T>(this IEnumerable<T> col, string sep) {
        return col.Where(e => e != null).Select(e => e.ToString()).Aggregate((a, b) => a + sep + b);
    }
}