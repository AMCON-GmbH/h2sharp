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
using System.Data.Common;

namespace System.Data.H2;

public sealed class H2Parameter : DbParameter {
    private DbType _dbType = DbType.Object;
    private ParameterDirection _direction = ParameterDirection.Input;
    private object _value;

    private H2Helper.Converter DotNetToJava;
    private bool isTypeSet;
    private int javaType;
    private object javaValue;

    public H2Parameter() { }

    public H2Parameter(string parameterName) {
        ParameterName = parameterName;
    }

    public H2Parameter(string parameterName, object value) {
        ParameterName = parameterName;
        Value = value;
    }

    public H2Parameter(object value) {
        Value = value;
    }

    public H2Parameter(string name, DbType dataType) {
        ParameterName = name;
        DbType = dataType;
    }

    public H2Parameter(string name, DbType dataType, int size) {
        ParameterName = name;
        DbType = dataType;
        Size = size;
    }

    public H2Parameter(string name, DbType dataType, int size, string sourceColumn) {
        ParameterName = name;
        DbType = dataType;
        Size = size;
        SourceColumn = sourceColumn;
    }

    public H2Parameter(
        string name,
        DbType dbType,
        int size,
        ParameterDirection direction,
        bool isNullable,
        byte precision,
        byte scale,
        string sourceColumn,
        DataRowVersion sourceVersion,
        object value) {
        ParameterName = name;
        DbType = dbType;
        Size = size;
        Direction = direction;
        IsNullable = isNullable;
        SourceColumn = sourceColumn;
        SourceVersion = sourceVersion;
        Value = value;
    }

    public override DbType DbType {
        get => _dbType;
        set {
            isTypeSet = true;
            _dbType = value;
            javaType = H2Helper.GetTypeCode(value);
        }
    }

    public override ParameterDirection Direction {
        get => _direction;
        set {
            if (value != ParameterDirection.Input) { throw new NotSupportedException(); }

            _direction = value;
        }
    }

    public override bool IsNullable { get; set; }
    public override string ParameterName { get; set; }
    public override int Size { get; set; }
    public override string SourceColumn { get; set; }
    public override bool SourceColumnNullMapping { get; set; }

    public override DataRowVersion SourceVersion { get; set; } = DataRowVersion.Current;

    public override object Value {
        get => _value;
        set {
            _value = value;

            if (value is DBNull || value == null) {
                javaValue = null;
            } else {
                if (DotNetToJava == null)
                    DotNetToJava = H2Helper.ConverterToJava(DbType);

                javaValue = DotNetToJava(value);
            }
        }
    }

    public override void ResetDbType() {
        _dbType = DbType.Object;
        isTypeSet = false;
    }

    internal void SetStatement(int ordnal, PreparedStatement statement) {
        if (isTypeSet) {
            statement.setObject(ordnal, javaValue, javaType);
        } else {
            statement.setObject(ordnal, javaValue);
        }
    }
}