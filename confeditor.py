import pandas as pd
import wx
import wx.dataview as dv
import sys
from pathlib import Path
from io import StringIO
import xml.etree.ElementTree as ET

from src import crag_helper

# class to redirect the console into a widget
class RedirectText(object):
    def __init__(self,aWxTextCtrl):
        self.out = aWxTextCtrl

    def write(self,string):
        self.out.WriteText(string)

class DlgAddKeyValue(wx.Dialog):
    def __init__(self, parent):
        super().__init__(parent, title="Ajouter une nouvelle key / value", size=(300, 200))

        self.panel = wx.Panel(self)

        self.key_label = wx.StaticText(self.panel, label="Key")
        self.key_text = wx.TextCtrl(self.panel)

        self.value_label = wx.StaticText(self.panel, label="Value")
        self.value_text = wx.TextCtrl(self.panel)

        self.ok_button = wx.Button(self.panel, wx.ID_OK, label="Add")
        self.cancel_button = wx.Button(self.panel, wx.ID_CANCEL, label="Cancel")

        self.sizer = wx.BoxSizer(wx.VERTICAL)
        self.sizer.Add(self.key_label, 0, wx.ALL, 5)
        self.sizer.Add(self.key_text, 0, wx.EXPAND | wx.ALL, 5)
        self.sizer.Add(self.value_label, 0, wx.ALL, 5)
        self.sizer.Add(self.value_text, 0, wx.EXPAND | wx.ALL, 5)
        self.sizer.Add(self.ok_button, 0, wx.ALIGN_CENTER | wx.ALL, 5)
        self.sizer.Add(self.cancel_button, 0, wx.ALIGN_CENTER | wx.ALL, 5)

        self.panel.SetSizerAndFit(self.sizer)
        self.Fit()


class PanelNode(wx.Panel):
    def __init__(self, parent):
        wx.Panel.__init__(self, parent)

        # DataViewListCtrl
        self.dvlc = dv.DataViewListCtrl(self, style=wx.LC_REPORT, size=(470, 250))

        # editable columns
        key_renderer = dv.DataViewTextRenderer(mode=dv.DATAVIEW_CELL_EDITABLE, align=wx.ALIGN_LEFT)
        value_renderer = dv.DataViewTextRenderer(mode=dv.DATAVIEW_CELL_EDITABLE, align=wx.ALIGN_LEFT)

        # columns
        self.dvlc.AppendColumn(dv.DataViewColumn("Key", key_renderer, 0, width=200))
        self.dvlc.AppendColumn(dv.DataViewColumn("Value", value_renderer, 1, width=200))

        # data model
        self.model = dv.DataViewListStore()
        self.dvlc.AssociateModel(self.model)

        # ui
        self.sizer = wx.BoxSizer(wx.VERTICAL)
        self.sizer.Add(self.dvlc, 1, wx.EXPAND)

        # button to add key / value
        button_add_key_value = wx.Button(self, label="Add key / value")
        button_add_key_value.Bind(wx.EVT_BUTTON, self.on_add_key_value)

        self.sizer.Add(button_add_key_value, 0, wx.ALL | wx.CENTER, 5)

    def load(self, dict_data):
        for key, value in dict_data.items():
            if key == "symbols":
                self.load_symbols(value)
            else:
                self.model.AppendItem([key, str(value)])

    def load_symbols(self, source):
        pass

    def reset(self):
        self.model.DeleteAllItems()

    def on_add_key_value(self, event):
        dialog = DlgAddKeyValue(self)
        if dialog.ShowModal() == wx.ID_OK:
            key = dialog.key_text.GetValue()
            value = dialog.value_text.GetValue()
            self.model.AppendItem([key, value])
        dialog.Destroy()


class PanelBroker(PanelNode):
    def __init__(self, parent):
        super().__init__(parent)

        self.SetSizer(self.sizer)

        self.Centre()


class PanelCrag(PanelNode):
    def __init__(self, parent):
        super().__init__(parent)

        self.SetSizer(self.sizer)

        self.Centre()


class PanelStrategy(PanelNode):
    def __init__(self, parent):
        super().__init__(parent)

        self.dvlc_symbols = dv.DataViewListCtrl(self, style=wx.LC_REPORT, size=(470, 5))

        # data model
        self.model_symbols = dv.DataViewListStore()
        self.dvlc_symbols.AssociateModel(self.model_symbols)

        # ui
        self.sizer.Add(self.dvlc_symbols, 1, wx.EXPAND)

        self.SetSizer(self.sizer)

        self.Centre()

    def reset(self):
        self.model.DeleteAllItems()
        self.model_symbols.DeleteAllItems()

        # clean previous dvlc_symbols
        while self.dvlc_symbols.GetColumnCount() > 0:
            column = self.dvlc_symbols.GetColumn(0)
            self.dvlc_symbols.DeleteColumn(column)

    def load_symbols(self, source):
        path = Path("./symbols/" + source)
        df_symbols = None
        if path.is_file():
            df_symbols = pd.read_csv(path)
        else:
            sourceIO = StringIO(source)
            df_symbols = pd.read_csv(sourceIO, sep=",")

        for col in df_symbols.columns:
            #self.dvlc_symbols.AppendTextColumn(col)
            renderer = dv.DataViewTextRenderer(mode=dv.DATAVIEW_CELL_EDITABLE, align=wx.ALIGN_LEFT)
            column = dv.DataViewColumn(col, renderer, len(self.dvlc_symbols.Columns))
            self.dvlc_symbols.AppendColumn(column)

        for _, row in df_symbols.iterrows():
            self.model_symbols.AppendItem([str(row[col]) for col in df_symbols.columns])

class MainPanel(wx.Panel):

    def __init__(self, parent):
        super().__init__(parent)
        main_sizer = wx.BoxSizer(wx.VERTICAL)
        self.SetSizer(main_sizer)

        # configuration file info
        self.staticTextUsdtEquity = wx.StaticText(self, label="configuration file : -", style=wx.ALIGN_LEFT)
        main_sizer.Add(self.staticTextUsdtEquity, 0, wx.ALL | wx.EXPAND, 5)

        sl1 = wx.StaticLine(self, size=(200, 1))
        main_sizer.Add(sl1, 0, wx.ALL | wx.EXPAND, 5)


        self.notebook = wx.Notebook(self)
        self.panel_strategy = PanelStrategy(self.notebook)
        self.notebook.AddPage(self.panel_strategy, "Strategy")
        self.panel_broker = PanelBroker(self.notebook)
        self.notebook.AddPage(self.panel_broker, "Broker")
        self.panel_crag = PanelCrag(self.notebook)
        self.notebook.AddPage(self.panel_crag, "Crag")
        main_sizer.Add(self.notebook, 0, wx.ALL | wx.EXPAND, 5)

        sl3 = wx.StaticLine(self, size=(200, 1))
        main_sizer.Add(sl3, 0, wx.ALL | wx.EXPAND, 5)

        # Console
        static_text_console = wx.StaticText(self, label="Console", style=wx.ALIGN_LEFT)
        main_sizer.Add(static_text_console, 0, wx.ALL | wx.EXPAND, 5)

        self.log = wx.TextCtrl(self, -1, size=(200, 100), style=wx.TE_MULTILINE|wx.TE_READONLY|wx.HSCROLL)
        main_sizer.Add(self.log,0, wx.ALL | wx.EXPAND, 5)
        sys.stdout = RedirectText(self.log)

    #
    # Events
    #
    def open_configuration_file(self, configuration_file):
        print("open configuration file : ", configuration_file)
        self.panel_crag.reset()
        self.panel_broker.reset()
        self.panel_strategy.reset()
        configuration = crag_helper.load_configuration_file(configuration_file)
        if not configuration:
            print("💥 A problem occurred while loading {}".format(configuration_file))
            self.staticTextUsdtEquity.SetLabel("File configuration : -")
        else:
            self.staticTextUsdtEquity.SetLabel("File configuration : " + configuration_file)
            self.panel_crag.load(configuration["crag"])
            self.panel_broker.load(configuration["broker"])
            self.panel_strategy.load(configuration["strategy"])

    def save_as_configuration_file(self, configuration_file):
        print("save configuration file : ", configuration_file)

        root = ET.Element("configuration")

        # strategy
        strategy = ET.SubElement(root, "strategy")
        strategy_params = ET.SubElement(strategy, "params")
        item_count = self.panel_strategy.model.GetCount()
        for row in range(item_count):
            key = self.panel_strategy.model.GetValueByRow(row, 0)
            value = self.panel_strategy.model.GetValueByRow(row, 1)
            if key == "id" or key == "name":
                strategy.set(str(key), str(value))
            else:
                strategy_params.set(str(key), str(value))

        # symbols
        columns = [self.panel_strategy.dvlc_symbols.GetColumn(col).GetTitle() for col in range(self.panel_strategy.dvlc_symbols.GetColumnCount())]

        data = []
        for row in range(self.panel_strategy.dvlc_symbols.GetItemCount()):
            row_data = [self.panel_strategy.dvlc_symbols.GetTextValue(row, col) for col in range(len(columns))]
            data.append(row_data)

        # Créer le DataFrame
        df = pd.DataFrame(data, columns=columns)
        strategy_params.set("symbols", df.to_csv(index=False))

        # broker
        broker = ET.SubElement(root, "broker")
        broker_params = ET.SubElement(broker, "params")
        item_count = self.panel_broker.model.GetCount()
        for row in range(item_count):
            key = self.panel_broker.model.GetValueByRow(row, 0)
            value = self.panel_broker.model.GetValueByRow(row, 1)
            if key == "name":
                broker.set(str(key), str(value))
            else:
                broker_params.set(str(key), str(value))

        # crag
        crag = ET.SubElement(root, "crag")
        item_count = self.panel_crag.model.GetCount()
        for row in range(item_count):
            key = self.panel_crag.model.GetValueByRow(row, 0)
            value = self.panel_crag.model.GetValueByRow(row, 1)
            crag.set(str(key), str(value))

        tree = ET.ElementTree(root)
        ET.indent(tree, space="\t", level=0)
        with open(configuration_file, "wb") as f:
            tree.write(f, encoding="utf-8", xml_declaration=True)

    def on_export_all_limit_orders(self, event):
        default_name = "selected_account" + "_limit_orders.csv"
        with wx.FileDialog(self,
                           "Open csv file",
                           defaultFile=default_name,
                           wildcard="csv files (*.csv)|*.csv",
                           style=wx.FD_OPEN) as fileDialog:
            if fileDialog.ShowModal() == wx.ID_CANCEL:
                return
            pathname = fileDialog.GetPath()
            try:
                with open(pathname, "w") as file:
                    header = ",".join(["a","b","c"])
                    file.write(header+"\n")
                    print(pathname+" saved")
            except IOError:
                wx.LogError("Cannot open file '%s'." % pathname)

#
class ConfEditorFrame(wx.Frame):

    def __init__(self):
        wx.Frame.__init__(self,
                          parent=None,
                          title='ConfEditor',
                          pos=wx.DefaultPosition,
                          size=(700, 800),
                          style=wx.SYSTEM_MENU | wx.CAPTION | wx.CLOSE_BOX)
        self.panel = MainPanel(self)
        self.create_menu()
        self.Show()

    def create_menu(self):
        menu_bar = wx.MenuBar()
        file_menu = wx.Menu()

        # Open
        item_open_configuration_file = file_menu.Append(wx.ID_ANY, 'Open configuration', 'Select a configuration file')
        item_save_as_configuration_file = file_menu.Append(wx.ID_ANY, 'Save configuration as...', 'Save the configuration file as...')
        menu_bar.Append(file_menu, '&File')
        self.SetMenuBar(menu_bar)

        self.Bind(event=wx.EVT_MENU, handler=self.on_open_configuration_file, source=item_open_configuration_file)
        self.Bind(event=wx.EVT_MENU, handler=self.on_save_as_configuration_file, source=item_save_as_configuration_file)

    #
    # Events
    #
    def on_open_configuration_file(self, event):
        title = "Select a configuration file"
        dlg = wx.FileDialog(self, title, style=wx.DD_DEFAULT_STYLE | wx.FD_FILE_MUST_EXIST, wildcard="XML (*.xml)|*.xml")
        if dlg.ShowModal() == wx.ID_OK:
            self.panel.open_configuration_file(dlg.GetPath())
        dlg.Destroy()

    def on_save_as_configuration_file(self, event):
        title = "Save the configuration file as..."
        dlg = wx.FileDialog(self, title, style=wx.DD_DEFAULT_STYLE, wildcard="XML (*.xml)|*.xml")
        if dlg.ShowModal() == wx.ID_OK:
            self.panel.save_as_configuration_file(dlg.GetPath())
        dlg.Destroy()


if __name__ == '__main__':
    app = wx.App(False)
    frame = ConfEditorFrame()
    app.MainLoop()
