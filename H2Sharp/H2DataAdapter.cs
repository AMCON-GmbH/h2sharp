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

using System.Data.Common;

namespace System.Data.H2;

public sealed class H2DataAdapter : DbDataAdapter, IDbDataAdapter {
    private static readonly object EventRowUpdated = new();
    private static readonly object EventRowUpdating = new();

    public H2DataAdapter() { }

    public H2DataAdapter(H2Command selectCommand) {
        this.SelectCommand = selectCommand;
    }

    public H2DataAdapter(string selectCommandText, string selectConnectionString)
        : this(selectCommandText, new H2Connection(selectConnectionString)) { }

    public H2DataAdapter(string selectCommandText, H2Connection selectConnection) {
        SelectCommand = selectConnection.CreateCommand();
        SelectCommand.CommandText = selectCommandText;
    }

    public new H2Command SelectCommand { get; set; }

    public new H2Command InsertCommand { get; set; }

    public new H2Command UpdateCommand { get; set; }

    public new H2Command DeleteCommand { get; set; }

    IDbCommand IDbDataAdapter.SelectCommand {
        get => SelectCommand;
        set => SelectCommand = (H2Command) value;
    }

    IDbCommand IDbDataAdapter.InsertCommand {
        get => InsertCommand;
        set => InsertCommand = (H2Command) value;
    }

    IDbCommand IDbDataAdapter.UpdateCommand {
        get => UpdateCommand;
        set => UpdateCommand = (H2Command) value;
    }

    IDbCommand IDbDataAdapter.DeleteCommand {
        get => DeleteCommand;
        set => DeleteCommand = (H2Command) value;
    }

    public event EventHandler<H2RowUpdatingEventArgs> RowUpdating {
        add => Events.AddHandler(EventRowUpdating, value);
        remove => Events.RemoveHandler(EventRowUpdating, value);
    }

    public event EventHandler<H2RowUpdatedEventArgs> RowUpdated {
        add => Events.AddHandler(EventRowUpdated, value);
        remove => Events.RemoveHandler(EventRowUpdated, value);
    }

    protected override RowUpdatedEventArgs CreateRowUpdatedEvent(DataRow dataRow,
                                                                 IDbCommand command,
                                                                 StatementType statementType,
                                                                 DataTableMapping tableMapping) {
        return new H2RowUpdatedEventArgs(dataRow, command, statementType, tableMapping);
    }

    protected override RowUpdatingEventArgs CreateRowUpdatingEvent(DataRow dataRow,
                                                                   IDbCommand command,
                                                                   StatementType statementType,
                                                                   DataTableMapping tableMapping) {
        return new H2RowUpdatingEventArgs(dataRow, command, statementType, tableMapping);
    }

    protected override void OnRowUpdating(RowUpdatingEventArgs value) {
        EventHandler<H2RowUpdatingEventArgs> handler = (EventHandler<H2RowUpdatingEventArgs>) Events[EventRowUpdating];

        if (null != handler) {
            handler(this, (H2RowUpdatingEventArgs) value);
        }
    }

    protected override void OnRowUpdated(RowUpdatedEventArgs value) {
        EventHandler<H2RowUpdatedEventArgs> handler = (EventHandler<H2RowUpdatedEventArgs>) Events[EventRowUpdated];

        if (null != handler) {
            handler(this, (H2RowUpdatedEventArgs) value);
        }
    }
}