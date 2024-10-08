import pandas as pd
import wx
import wx.dataview as dv


class DataFrameController(dv.DataViewListCtrl):
    def __init__(self, parent):
        super(DataFrameController, self).__init__(parent,
                                                  style=wx.dataview.DV_ROW_LINES | wx.dataview.DV_VERT_RULES,
                                                  size=(500, 300))

        self.df = None  # dataframe

        '''
        # add columns
        for index, col_name in enumerate(self.df.columns):
            editable_renderer = dv.DataViewTextRenderer(mode=dv.DATAVIEW_CELL_EDITABLE)
            column = dv.DataViewColumn(col_name, editable_renderer, index, width=80)
            self.AppendColumn(column)

        self.Bind(dv.EVT_DATAVIEW_ITEM_VALUE_CHANGED, self.on_value_changed)

        # fill the controller with data from dataframe
        self.populate_list(self.df)
        '''
        self.Centre()
        self.Show(True)

    def set_dataframe(self, dataframe):
        self.DeleteAllItems()
        self.df = dataframe

        # add columns
        for index, col_name in enumerate(self.df.columns):
            editable_renderer = dv.DataViewTextRenderer(mode=dv.DATAVIEW_CELL_EDITABLE)
            column = dv.DataViewColumn(col_name, editable_renderer, index, width=80)
            self.AppendColumn(column)

        self.Bind(dv.EVT_DATAVIEW_ITEM_VALUE_CHANGED, self.on_value_changed)

        # fill the controller with data from dataframe
        self.populate_list(self.df)

    def populate_list(self, dataframe):
        for index, row in dataframe.iterrows():
            self.AppendItem([str(item) for item in row.tolist()])

    def on_value_changed(self, event):
        item = event.GetItem()
        row = self.ItemToRow(item)

        # get the index of the modified column
        col = event.GetColumn()

        # get the new value
        new_value = self.GetValue(row, col)
        print(f"line {row}, column {col} => {new_value}")

        # update the dataframe
        self.df.iloc[row, col] = new_value
        print(self.df)


class MyFrame(wx.Frame):
    def __init__(self, parent, title):
        super(MyFrame, self).__init__(parent, title=title, size=(500, 300))

        data = {
            'Nom': ['Alice', 'Bob', 'Charlie'],
            'Age': [25, 30, 35],
            'Ville': ['Paris', 'Lyon', 'Marseille']
        }
        dataframe = pd.DataFrame(data)

        panel = wx.Panel(self)
        vbox = wx.BoxSizer(wx.VERTICAL)

        # init DataViewListCtrl
        self.dvlc = DataFrameController(panel)
        vbox.Add(self.dvlc, 1, wx.EXPAND | wx.ALL, 10)

        self.dvlc.set_dataframe(dataframe)

        ok_button = wx.Button(panel, label='OK')
        vbox.Add(ok_button, 0, wx.ALIGN_CENTER | wx.ALL, 10)

        panel.SetSizer(vbox)

        # event
        ok_button.Bind(wx.EVT_BUTTON, self.on_ok_button)

    def on_ok_button(self, event):
        self.Close()


class MyApp(wx.App):
    def OnInit(self):

        frame = MyFrame(None, title="DataFrame in DataViewListCtrl")
        frame.Show()
        return True


if __name__ == '__main__':
    app = MyApp(False)
    app.MainLoop()
