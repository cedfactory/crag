import pandas as pd
import wx
import sys

from src.utils import settings_helper
from src import broker_bitget_api,utils

# class to redirect the console into a widget
class RedirectText(object):
    def __init__(self,aWxTextCtrl):
        self.out = aWxTextCtrl

    def write(self,string):
        self.out.WriteText(string)

#
class MainPanel(wx.Panel):

    #
    # UI
    #
    def __init__(self, parent):
        super().__init__(parent)
        main_sizer = wx.BoxSizer(wx.VERTICAL)

        # Accounts
        staticTextAccounts = wx.StaticText(self,label = "Accounts", style = wx.ALIGN_LEFT)
        main_sizer.Add(staticTextAccounts,0, wx.ALL | wx.EXPAND, 5)

        self.accounts = wx.ComboBox(self,choices = [])
        main_sizer.Add(self.accounts, 0, wx.ALL | wx.EXPAND, 5)

        self.accounts.Bind(wx.EVT_COMBOBOX, self.on_account)

        # usdt equity
        self.staticTextUsdtEquity = wx.StaticText(self, label="USDT Equity : ", style=wx.ALIGN_LEFT)
        main_sizer.Add(self.staticTextUsdtEquity, 0, wx.ALL | wx.EXPAND, 5)

        sl1 = wx.StaticLine(self, size=(200, 1))
        main_sizer.Add(sl1, 0, wx.ALL | wx.EXPAND, 5)

        # Positions
        staticTextPositions = wx.StaticText(self,label = "Positions", style = wx.ALIGN_LEFT)
        main_sizer.Add(staticTextPositions,0, wx.ALL | wx.EXPAND, 5)

        self.positions = wx.ListCtrl(
            self, size=(-1, 100),
            style=wx.LC_REPORT | wx.BORDER_SUNKEN
        )
        self.positions.InsertColumn(0, 'Symbol', width=140)
        self.positions.InsertColumn(1, 'USDT Equity', width=140)
        self.positions.InsertColumn(2, 'Side', width=50)
        self.positions.InsertColumn(3, 'Leverage', width=70)
        main_sizer.Add(self.positions, 0, wx.ALL | wx.EXPAND, 5)

        sl2 = wx.StaticLine(self, size=(200, 1))
        main_sizer.Add(sl2, 0, wx.ALL | wx.EXPAND, 5)

        # Orders
        staticTextOrders = wx.StaticText(self, label="Orders", style=wx.ALIGN_LEFT)
        main_sizer.Add(staticTextOrders, 0, wx.ALL | wx.EXPAND, 5)

        self.orders = wx.ListCtrl(
            self, size=(-1, 100),
            style=wx.LC_REPORT | wx.BORDER_SUNKEN
        )
        self.orders.InsertColumn(0, 'Symbol', width=140)
        self.orders.InsertColumn(1, 'Side', width=80)
        self.orders.InsertColumn(2, 'Price', width=70)
        self.orders.InsertColumn(3, 'Leverage', width=70)
        self.orders.InsertColumn(4, 'MarginCoin', width=70)
        self.orders.InsertColumn(5, 'ClientOid', width=130)
        self.orders.InsertColumn(6, 'OrderId', width=130)
        main_sizer.Add(self.orders, 0, wx.ALL | wx.EXPAND, 5)

        cancel_order_button = wx.Button(self, label='Cancel open order')
        cancel_order_button.Bind(wx.EVT_BUTTON, self.on_cancel_order)
        main_sizer.Add(cancel_order_button, 0, wx.ALL | wx.CENTER, 5)

        sl3 = wx.StaticLine(self, size=(200, 1))
        main_sizer.Add(sl3, 0, wx.ALL | wx.EXPAND, 5)

        # Order
        order_button = wx.Button(self, label='Order')
        order_button.Bind(wx.EVT_BUTTON, self.on_order)
        main_sizer.Add(order_button, 0, wx.ALL | wx.CENTER, 5)
        self.SetSizer(main_sizer)

        # Console
        staticTextConsole = wx.StaticText(self,label = "Console", style = wx.ALIGN_LEFT)
        main_sizer.Add(staticTextConsole,0, wx.ALL | wx.EXPAND, 5)

        self.log = wx.TextCtrl(self, -1, size=(200, 150), style=wx.TE_MULTILINE|wx.TE_READONLY|wx.HSCROLL)
        main_sizer.Add(self.log,0, wx.ALL | wx.EXPAND, 5)
        redir = RedirectText(self.log)
        sys.stdout = redir

    #
    # Events
    #
    def open_settings_file(self, settings_file):
        accounts_info = settings_helper.get_accounts_info()
        accounts = list(accounts_info.keys())
        print("loading accounts : ", accounts)
        self.accounts.Clear()
        self.accounts.Append(accounts)


    def get_broker_from_selected_account(self):
        selected_account = self.accounts.GetString(self.accounts.GetSelection())
        account_info = settings_helper.get_account_info(selected_account)
        broker_name = account_info.get("broker", "")
        my_broker = None
        if broker_name == "bitget":
            my_broker = broker_bitget_api.BrokerBitGetApi(
                {"account": selected_account, "reset_account": "False"})
        return my_broker

    def on_account(self, event):
        positions = []
        orders = []
        usdt_equity = 0

        my_broker = self.get_broker_from_selected_account()
        if my_broker:
            positions = my_broker.get_open_position()
            orders = my_broker.get_open_orders(["XRP"])
            usdt_equity = my_broker.get_usdt_equity()

        # update usdt equity
        print("usdt equity : ", usdt_equity)
        self.staticTextUsdtEquity.SetLabel("USDT Equity : "+utils.KeepNDecimals(usdt_equity))

        # update positions
        print("positions : ", positions)
        self.positions.DeleteAllItems()
        if isinstance(positions, pd.DataFrame):
            for index, row in positions.iterrows():
                self.positions.Append([row["symbol"], utils.KeepNDecimals(row["usdtEquity"]), row["holdSide"], row["leverage"]])

        # update orders
        print("orders : ", orders)
        self.orders.DeleteAllItems()
        if isinstance(orders, pd.DataFrame):
            for index, row in orders.iterrows():
                self.orders.Append([row["symbol"], row["side"], row["price"], row["leverage"], row["marginCoin"], row["clientOid"], row["orderId"]])

    def on_cancel_order(self, event):
        index = self.orders.GetFirstSelected()
        if index == -1:
            return
        symbol = self.orders.GetItem(index, col=0).GetText()
        marginCoin = self.orders.GetItem(index, col=4).GetText()
        orderId = self.orders.GetItem(index, col=6).GetText()
        print("cancel open order : ", orderId)
        my_broker = self.get_broker_from_selected_account()
        my_broker.cancel_order(symbol, marginCoin, orderId)

    def on_order(self, event):
        print('in on_order')

#
class CragFrame(wx.Frame):

    def __init__(self):
        wx.Frame.__init__(self, parent=None, title='Crag UI',pos=wx.DefaultPosition,size=(600, 650), style= wx.SYSTEM_MENU | wx.CAPTION | wx.CLOSE_BOX)
        self.panel = MainPanel(self)
        self.create_menu()
        self.Show()

    def create_menu(self):
        menu_bar = wx.MenuBar()
        file_menu = wx.Menu()
        open_settings_file_menu_item = file_menu.Append(wx.ID_ANY, 'Load settings', 'Select a settings file')
        menu_bar.Append(file_menu, '&File')
        self.Bind(
            event=wx.EVT_MENU, 
            handler=self.on_open_settings_file,
            source=open_settings_file_menu_item,
        )
        self.SetMenuBar(menu_bar)

    def load_accounts(self, filepath=""):
        self.panel.open_settings_file(filepath)

    #
    # Events
    #
    def on_open_settings_file(self, event):
        title = "Select a settings file"
        dlg = wx.FileDialog(self, title, style=wx.DD_DEFAULT_STYLE)
        if dlg.ShowModal() == wx.ID_OK:
            self.load_accounts(dlg.GetPath())
        dlg.Destroy()

#
if __name__ == '__main__':
    app = wx.App(False)
    frame = CragFrame()
    frame.load_accounts()
    app.MainLoop()
