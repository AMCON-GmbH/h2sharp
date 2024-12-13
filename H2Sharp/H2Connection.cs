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
using org.h2.tools;
using System.Data.Common;
using Driver = org.h2.Driver;

namespace System.Data.H2;

public sealed class H2Connection : DbConnection {
    internal Connection connection;

    private string connectionString;
    private string password;
    private readonly H2ConnectionPool pool;
    internal H2Transaction transaction;
    private string userName;

    static H2Connection() {
        Driver.load();

        // To Start the servers prevents the program from stop.
        // I don't know why this code exists, but if anyone need it, it stays commented out.
        //_ = Server.createWebServer().start();
        //Server.createTcpServer().start();
        //org.h2.tools.Server.openBrowser("http://localhost:8082");
    }

    public H2Connection() { }

    public H2Connection(string connectionString) {
        this.connectionString = connectionString;
    }

    public H2Connection(string connectionString, string userName, string password) {
        this.connectionString = connectionString;
        this.userName = userName;
        this.password = password;
    }

    internal H2Connection(Connection self) {
        connection = self;

        if (H2Helper.GetAdoTransactionLevel(self.getTransactionIsolation()) != IsolationLevel.Unspecified) {
            transaction = new H2Transaction(this);
        }
    }

    internal H2Connection(H2ConnectionPool pool) {
        this.pool = pool;
    }

    public override string DataSource => throw new NotImplementedException();

    public override string ServerVersion => throw new NotImplementedException();

    public override string ConnectionString {
        get => connectionString;
        set {
            if (IsOpen) { throw new InvalidOperationException(); }

            connectionString = value;
        }
    }

    public string UserName {
        get => userName;
        set {
            if (IsOpen) { throw new InvalidOperationException(); }

            userName = value;
        }
    }

    public string Password {
        get => password;
        set {
            if (IsOpen) { throw new InvalidOperationException(); }

            password = value;
        }
    }

    public override ConnectionState State {
        get {
            if (IsOpen) {
                return ConnectionState.Open;
            }

            if (connection == null) { return ConnectionState.Closed; }

            return ConnectionState.Open;
        }
    }

    public override string Database => throw new NotImplementedException();

    public bool IsOpen => connection != null;

    public override void ChangeDatabase(string databaseName) {
        throw new NotImplementedException();
    }

    public override void Close() {
        Dispose();
    }

    public override void Open() {
        if (userName == null || password == null) {
            if (IsOpen) { throw new InvalidOperationException("connection is already open"); }

            try {
                if (pool != null) {
                    connection = pool.GetConnection();
                } else {
                    connection = DriverManager.getConnection(connectionString);
                }
            } catch (JdbcSQLException ex) {
                throw new H2Exception(ex);
            }
        } else {
            Open(userName, password);
        }
    }

    public void Open(string userName, string password) {
        if (userName == null) { throw new ArgumentNullException("userName"); }

        if (password == null) { throw new ArgumentNullException("password"); }

        if (IsOpen) { throw new InvalidOperationException("connection is already open"); }

        try {
            if (pool != null) {
                connection = pool.GetConnection(userName, password);
            } else {
                connection = DriverManager.getConnection(connectionString, userName, password);
            }
        } catch (JdbcSQLException ex) {
            throw new H2Exception(ex);
        }
    }

    public new H2Command CreateCommand() {
        return new H2Command(this);
    }

    public new H2Transaction BeginTransaction() {
        return BeginTransaction(IsolationLevel.ReadCommitted);
    }

    public new H2Transaction BeginTransaction(IsolationLevel isolationLevel) {
        CheckIsOpen();

        if (isolationLevel == IsolationLevel.Unspecified) {
            isolationLevel = IsolationLevel.ReadCommitted;
        }

        if (transaction != null) { throw new InvalidOperationException(); }

        try {
            connection.setTransactionIsolation(H2Helper.GetJdbcTransactionLevel(isolationLevel));
        } catch (JdbcSQLException ex) {
            throw new H2Exception(ex);
        }

        transaction = new H2Transaction(this);
        return transaction;
    }

    internal void CheckIsOpen() {
        if (!IsOpen) { throw new InvalidOperationException("must open the connection first"); }
    }

    protected override DbCommand CreateDbCommand() {
        return CreateCommand();
    }

    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) {
        return BeginTransaction(isolationLevel);
    }

    protected override void Dispose(bool disposing) {
        base.Dispose(disposing);

        if (disposing) {
            if (IsOpen) {
                if (transaction != null) {
                    transaction.Dispose();
                    transaction = null;
                }

                if (pool != null) {
                    pool.Enqueue(connection);
                    connection = null;
                } else {
                    connection.close();
                    connection = null;
                }
            }

            userName = null;
            password = null;
        }
    }

    public override DataTable GetSchema() {
        return new DataTable();
    }

    public override DataTable GetSchema(string collectionName) {
        return new DataTable();
    }

    public override DataTable GetSchema(string collectionName, string[] restrictionValues) {
        return new DataTable();
    }
}