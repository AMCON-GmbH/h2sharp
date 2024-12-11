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
using org.h2.api;

namespace System.Data.H2.Api;

/// <summary>
///     A user-defined aggregate function needs to implement this interface.
///     The class must be public and must have a public non-argument constructor.
/// </summary>
public abstract class H2AggregateFunction : AggregateFunction {
    /// <summary>
    ///     the computed aggregate value.
    /// </summary>
    protected abstract object Result { get; }

    void AggregateFunction.add(object value) {
        OnAdd(value);
    }

    object AggregateFunction.getResult() {
        return Result;
    }

    int AggregateFunction.getType(int[] inputType) {
        return OnGetType(inputType);
    }

    void AggregateFunction.init(Connection conn) {
        var connection = new H2Connection(conn);
        OnInit(connection);
    }

    /// <summary>
    ///     This method is called once for each row.
    ///     If the aggregate function is called with multiple parameters,
    ///     those are passed as array.
    /// </summary>
    /// <param name="value">the value(s) for this row</param>
    protected abstract void OnAdd(object value);

    /// <summary>
    ///     This method must return the SQL type of the method,
    ///     given the SQL type of the input data.
    ///     The method should check here if the number of parameters passed is correct,
    ///     and if not it should throw an exception.
    /// </summary>
    /// <param name="inputType">the SQL type of the parameters</param>
    /// <returns>the SQL type of the result</returns>
    protected abstract int OnGetType(int[] inputType);

    /// <summary>
    ///     This method is called when the aggregate function is used.
    ///     A new object is created for each invocation.
    /// </summary>
    /// <param name="connection">conn a connection to the database</param>
    /// <returns></returns>
    protected abstract int OnInit(H2Connection connection);
}