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
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;

namespace System.Data.H2;

public sealed class H2DataReader : DbDataReader {
    private static readonly DateTime UTCStart = new(1970, 1, 1);

    private readonly H2Connection connection;

    private H2Helper.Converter[] converters;
    private ResultSetMetaData meta;
    private readonly ResultSet set;

    private Type[] types;

    internal H2DataReader(H2Connection connection, ResultSet set) {
        this.set = set;
        this.connection = connection;
    }

    private ResultSetMetaData Meta {
        get {
            if (meta == null) {
                meta = set.getMetaData();
            }

            return meta;
        }
    }

    public override int RecordsAffected {
        get {
            int mark = set.getRow();
            set.last();
            int rowCount = set.getRow();
            set.absolute(mark);
            return rowCount;
        }
    }

    public override bool HasRows {
        get {
            bool r = set.next();
            set.previous();
            return r;
        }
    }

    public override bool IsClosed => set.isClosed();

    public override object this[string name] => GetValue(GetOrdinal(name));

    public override object this[int ordinal] => GetValue(ordinal);

    public override int Depth => Meta.getColumnCount();

    public override int FieldCount => Meta.getColumnCount();

    private static int ConvertOrdnal(int ordinal) {
        if (ordinal == int.MaxValue) { throw new H2Exception("invalid ordinal"); }

        return ordinal + 1;
    }

    public override bool IsDBNull(int ordinal) {
        return set.getObject(ConvertOrdnal(ordinal)) == null;
    }

    public override bool NextResult() {
        return set.next();
    }

    public override bool GetBoolean(int ordinal) {
        return set.getBoolean(ConvertOrdnal(ordinal));
    }

    public override byte GetByte(int ordinal) {
        return set.getByte(ConvertOrdnal(ordinal));
    }

    public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length) {
        byte[] rv = set.getBytes(ConvertOrdnal(ordinal));
        Array.Copy(rv, dataOffset, buffer, bufferOffset, length);
        return length;
    }

    public override char GetChar(int ordinal) {
        return set.getString(ConvertOrdnal(ordinal))[0];
    }

    public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length) {
        string v = GetString(ordinal);
        long v1 = dataOffset + length > v.Length ? v.Length - dataOffset : length;
        char[] vs = v.ToCharArray(Convert.ToInt32(dataOffset), Convert.ToInt32(v1));
        vs.CopyTo(buffer, bufferOffset);
        return v1;
    }

    public override string GetDataTypeName(int ordinal) {
        return Meta.getColumnTypeName(ConvertOrdnal(ordinal));
    }

    public override DateTime GetDateTime(int ordinal) {

        return UTCStart.AddMilliseconds(set.getDate(ordinal).getTime());
    }

    public override decimal GetDecimal(int ordinal) {
        return (decimal) set.getObject(ConvertOrdnal(ordinal));
    }

    public override double GetDouble(int ordinal) {
        return set.getDouble(ConvertOrdnal(ordinal));
    }

    public override IEnumerator GetEnumerator() {
        return GetEnumerator();
    }

    public override Type GetFieldType(int ordinal) {
        if (types == null)
            types = new Type[Meta.getColumnCount() + 1];

        Type? type = types[ordinal];

        if (type == null)
            types[ordinal] = type = DoGetFieldType(ordinal);

        return type;
    }

    private Type DoGetFieldType(int ordinal) {
        int typeCode = Meta.getColumnType(ConvertOrdnal(ordinal));
        return H2Helper.GetType(typeCode);
    }

    public override float GetFloat(int ordinal) {
        return set.getFloat(ConvertOrdnal(ordinal));
    }

    public override Guid GetGuid(int ordinal) {
        return new Guid(GetString(ordinal));
    }

    public override short GetInt16(int ordinal) {
        return set.getShort(ConvertOrdnal(ordinal));
    }

    public override int GetInt32(int ordinal) {
        return set.getInt(ConvertOrdnal(ordinal));
    }

    public override long GetInt64(int ordinal) {
        return set.getLong(ConvertOrdnal(ordinal));
    }

    public override string GetName(int ordinal) {
        int i = ConvertOrdnal(ordinal);
        string? s = Meta.getColumnLabel(i);
        return s == null ? Meta.getColumnName(i) : s;
    }

    public override int GetOrdinal(string name) {
        for (int index = 1; index <= Meta.getColumnCount(); ++index) {
            if (Meta.getColumnName(index).ToUpper() == name.ToUpper()) {
                return index - 1;
            }
        }

        return -1;
    }

    public override DataTable GetSchemaTable() {
        /*
        JDBC reference :
        http://java.sun.com/j2se/1.5.0/docs/api/java/sql/ResultSetMetaData.html

        ADO.NET reference :
        http://msdn.microsoft.com/en-us/library/system.data.sqlclient.sqldatareader.getschematable.aspx
        */
        var table = new DataTable();
        DataColumn? ColumnName = table.Columns.Add("ColumnName", typeof(string));
        DataColumn? ColumnOrdinal = table.Columns.Add("ColumnOrdinal", typeof(int));
        DataColumn? ColumnSize = table.Columns.Add("ColumnSize", typeof(int));
        DataColumn? NumericPrecision = table.Columns.Add("NumericPrecision", typeof(int));
        DataColumn? NumericScale = table.Columns.Add("NumericScale", typeof(int));
        DataColumn? IsUnique = table.Columns.Add("IsUnique", typeof(bool));
        DataColumn? IsKey = table.Columns.Add("IsKey", typeof(bool));
        DataColumn? BaseServerName = table.Columns.Add("BaseServerName", typeof(string));
        DataColumn? BaseCatalogName = table.Columns.Add("BaseCatalogName", typeof(string));
        DataColumn? BaseColumnName = table.Columns.Add("BaseColumnName", typeof(string));
        DataColumn? BaseSchemaName = table.Columns.Add("BaseSchemaName", typeof(string));
        DataColumn? BaseTableName = table.Columns.Add("BaseTableName", typeof(string));
        DataColumn? DataType = table.Columns.Add("DataType", typeof(Type));
        DataColumn? AllowDBNull = table.Columns.Add("AllowDBNull", typeof(bool));
        DataColumn? ProviderType = table.Columns.Add("ProviderType");
        DataColumn? IsAliased = table.Columns.Add("IsAliased", typeof(bool));
        DataColumn? IsExpression = table.Columns.Add("IsExpression", typeof(bool));
        DataColumn? IsIdentity = table.Columns.Add("IsIdentity", typeof(bool));
        DataColumn? IsAutoIncrement = table.Columns.Add("IsAutoIncrement", typeof(bool));
        DataColumn? IsRowVersion = table.Columns.Add("IsRowVersion", typeof(bool));
        DataColumn? IsHidden = table.Columns.Add("IsHidden", typeof(bool));
        DataColumn? IsLong = table.Columns.Add("IsLong", typeof(bool));
        DataColumn? IsReadOnly = table.Columns.Add("IsReadOnly", typeof(bool));
        DataColumn? ProviderSpecificDataType = table.Columns.Add("ProviderSpecificDataType");
        DataColumn? DataTypeName = table.Columns.Add("DataTypeName", typeof(string));
        DataColumn? DbType = table.Columns.Add("DbType", typeof(DbType)); // not standard !!!
        //var XmlSchemaCollectionDatabase = table.Columns.Add("XmlSchemaCollectionDatabase");
        //var XmlSchemaCollectionOwningSchema = table.Columns.Add("XmlSchemaCollectionOwningSchema");
        //var XmlSchemaCollectionName = table.Columns.Add("XmlSchemaCollectionName");

        //var dbMeta = connection.connection.getMetaData();
        var tablesPksAndUniques = new Dictionary<string, KeyValuePair<HashSet<string>, HashSet<string>>>();
        ResultSetMetaData? meta = Meta;

        int nCols = meta.getColumnCount();
        table.MinimumCapacity = nCols;

        for (int iCol = 1; iCol <= nCols; iCol++) {
            // Beware : iCol starts at 1 (JDBC convention)
            DataRow? row = table.NewRow();
            string? name = meta.getColumnName(iCol);
            string? label = meta.getColumnLabel(iCol);
            string? tableName = meta.getTableName(iCol);

            KeyValuePair<HashSet<string>, HashSet<string>> pksAndUniques;

            if (!tablesPksAndUniques.TryGetValue(tableName, out pksAndUniques)) {
                pksAndUniques = new KeyValuePair<HashSet<string>, HashSet<string>>(
                    connection.GetPrimaryKeysColumns(tableName),
                    connection.GetUniqueColumns(tableName)
                );
            }

            row[ColumnName] = label != null ? label : name;
            row[ColumnOrdinal] = iCol - 1;
            row[BaseColumnName] = name;
            row[BaseSchemaName] = meta.getSchemaName(iCol);
            row[BaseTableName] = tableName;
            row[ColumnSize] = meta.getColumnDisplaySize(iCol);
            row[IsReadOnly] = meta.isReadOnly(iCol);
            row[IsKey] = pksAndUniques.Key.Contains(name);
            row[IsUnique] = pksAndUniques.Value.Contains(name);
            row[DataTypeName] = meta.getColumnTypeName(iCol); // TODO check this !
            row[NumericPrecision] = meta.getPrecision(iCol);
            row[NumericScale] = meta.getScale(iCol);
            int jdbcType = meta.getColumnType(iCol);
            Type? type = H2Helper.GetType(jdbcType);
            DbType dbType = H2Helper.GetDbType(jdbcType);
            row[DataType] = type;
            row[DbType] = dbType;
            row[AllowDBNull] = meta.isNullable(iCol);
            table.Rows.Add(row);
        }

        return table;
    }

    public override string GetString(int ordinal) {
        return set.getString(ConvertOrdnal(ordinal));
    }

    public override object GetValue(int ordinal) {
        int convOrd = ConvertOrdnal(ordinal);
        object result = set.getObject(convOrd);

        if (result == null)
            return DBNull.Value;

        if (converters == null)
            converters = new H2Helper.Converter[Meta.getColumnCount()];

        H2Helper.Converter converter = converters[ordinal];

        if (converter == null)
            converters[ordinal] = converter = H2Helper.ConverterToCLR(Meta.getColumnType(convOrd));

        return converter(result);
    }

    public override int GetValues(object[] values) {
        if (values == null) { throw new ArgumentNullException("values"); }

        for (int index = 0; index < values.Length; ++index) {
            values[index] = GetValue(index);
        }

        return values.Length;
    }

    public override bool Read() {
        return set.next();
    }

    public override void Close() {
        set.close();
    }
}

internal static class DatabaseMetaDataExtensions {
    public static Dictionary<string, int> GetColumnTypeCodes(this H2Connection connection, string tableName) {
        // Reference : http://java.sun.com/javase/6/docs/api/java/sql/DatabaseMetaData.html#getPrimaryKeys(java.lang.String, java.lang.String, java.lang.String)
        /*try {
            var dbMeta = connection.connection.getMetaData();
            var res = dbMeta != null ? dbMeta.getColumns(null, null, tableName, null) : null;
            if (res != null) {
                var ret = new Dictionary<String, int>();
                while (res.next()) {
                    var columnName = res.getString(4);
                    var colType = res.getInt(5);
                    ret[columnName] = colType;
                }
                return ret;
            }
        } catch (Exception ex) {
            Console.WriteLine(ex);
        }*/
        return connection.ReadMap<int>(
            "select column_name, data_type from INFORMATION_SCHEMA.COLUMNS where upper(table_name) = '" + tableName.ToUpper() + "'"
        );
    }

    public static HashSet<string> GetPrimaryKeysColumns(this H2Connection connection, string tableName) {
        // Reference : http://java.sun.com/javase/6/docs/api/java/sql/DatabaseMetaData.html#getPrimaryKeys(java.lang.String, java.lang.String, java.lang.String)
        /*try {
            var dbMeta = connection.connection.getMetaData();
            var res = dbMeta != null ? dbMeta.getPrimaryKeys(null, null, tableName) : null;
            if (res != null) {
                var ret = new HashSet<String>();
                while (res.next()) {
                    var columnName = res.getString(4);
                    ret.Add(columnName);
                }
                return ret;
            }
        } catch (Exception ex) {
            Console.WriteLine(ex);
        }*/
        var ret = new HashSet<string>();

        foreach (string? list in connection.ReadStrings(
                "select column_list from INFORMATION_SCHEMA.CONSTRAINTS where constraint_type = 'PRIMARY KEY' and upper(table_name) = '"
                + tableName.ToUpper()
                + "' "
            )) {
            foreach (string? col in list.Split(','))
                ret.Add(col.Trim());
        }

        return ret;
    }

    public static HashSet<string> GetUniqueColumns(this H2Connection connection, string tableName) {
        // Reference : http://java.sun.com/javase/6/docs/api/java/sql/DatabaseMetaData.html#getIndexInfo(java.lang.String, java.lang.String, java.lang.String, boolean, boolean)
        /*try {
            var dbMeta = connection.connection.getMetaData();
            var res = dbMeta != null ? dbMeta.getIndexInfo(null, null, tableName, true, false) : null;
            if (res != null) {
                var ret = new HashSet<String>();
                while (res.next()) {
                    var columnName = res.getString(4);
                    ret.Add(columnName);
                }
                return ret;
            }
        } catch (Exception ex) {
            Console.WriteLine(ex);
        }*/
        return new HashSet<string>(
            connection.ReadStrings(
                "select column_list from INFORMATION_SCHEMA.CONSTRAINTS where constraint_type = 'UNIQUE' and upper(table_name) = '"
                + tableName.ToUpper()
                + "'"
            )
        );
    }
}