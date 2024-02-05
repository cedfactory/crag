import pandas as pd
import wx
import wx.lib.agw.floatspin as FS
import sys

from src.utils import settings_helper
from src import broker_bitget_api,utils,trade

# class to redirect the console into a widget
class RedirectText(object):
    def __init__(self,aWxTextCtrl):
        self.out = aWxTextCtrl

    def write(self,string):
        self.out.WriteText(string)

class PanelPositions(wx.Panel):
    def __init__(self, parent, main):
        wx.Panel.__init__(self, parent)

        main_sizer = wx.BoxSizer(wx.VERTICAL)
        self.SetSizer(main_sizer)

        self.main = main

        # Positions
        self.positions = wx.ListCtrl(
            self, size=(570, 200),
            style=wx.LC_REPORT | wx.BORDER_SUNKEN
        )
        self.positions.InsertColumn(0, 'Symbol', width=140)
        self.positions.InsertColumn(1, 'USDT Equity', width=100)
        self.positions.InsertColumn(2, 'Side', width=50)
        self.positions.InsertColumn(3, 'Leverage', width=70)
        self.positions.InsertColumn(4, 'unrealizedPL', width=90)
        main_sizer.Add(self.positions, 0, wx.ALL | wx.EXPAND, 5)

        hsizer = wx.BoxSizer(wx.HORIZONTAL)
        close_position_button = wx.Button(self, label='Close selected position')
        close_position_button.Bind(wx.EVT_BUTTON, self.main.on_close_position)
        hsizer.Add(close_position_button, 0, wx.ALL | wx.EXPAND, 5)

        close_all_positions_button = wx.Button(self, label='Close all positions')
        close_all_positions_button.Bind(wx.EVT_BUTTON, self.main.on_close_all_positions)
        hsizer.Add(close_all_positions_button, 0, wx.ALL | wx.EXPAND, 5)
        main_sizer.Add(hsizer, 0, wx.ALL | wx.CENTER, 5)

        # open position
        hsizer = wx.BoxSizer(wx.HORIZONTAL)
        staticOpenPosition = wx.StaticText(self,label = "Open position :", style = wx.ALIGN_LEFT)
        hsizer.Add(staticOpenPosition,0, wx.ALL | wx.EXPAND, 5)
        self.symbols = wx.ComboBox(self,choices = ["BTC", "ETH", "XRP"])
        hsizer.Add(self.symbols,0, wx.ALL | wx.EXPAND, 5)
        staticAmount = wx.StaticText(self,label = "Amount ($)", style = wx.ALIGN_LEFT)
        hsizer.Add(staticAmount,0, wx.ALL | wx.EXPAND, 5)
        self.amount = FS.FloatSpin(self, -1, size=wx.Size(70, -1), min_val=0, increment=0.1, value=0., digits=3, agwStyle=FS.FS_LEFT)
        hsizer.Add(self.amount, 0, wx.ALL | wx.CENTER, 5)
        staticLeverage = wx.StaticText(self, label="Leverage", style=wx.ALIGN_LEFT)
        hsizer.Add(staticLeverage, 0, wx.ALL | wx.EXPAND, 5)
        self.leverage = FS.FloatSpin(self, -1, size=wx.Size(40, -1), min_val=1, increment=1, value=2, digits=0, agwStyle=FS.FS_LEFT)
        hsizer.Add(self.leverage, 0, wx.ALL | wx.CENTER, 5)
        open_position_button = wx.Button(self, label="Open position")
        open_position_button.Bind(wx.EVT_BUTTON, self.main.on_open_position)
        hsizer.Add(open_position_button, 0, wx.ALL | wx.CENTER, 5)
        main_sizer.Add(hsizer, 0, wx.ALL | wx.LEFT, 5)


class PanelOrders(wx.Panel):
    def __init__(self, parent, main):
        wx.Panel.__init__(self, parent)
        main_sizer = wx.BoxSizer(wx.VERTICAL)
        self.SetSizer(main_sizer)

        self.main = main

        self.orders = wx.ListCtrl(
            self, size=(570, 200),
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

        hsizer = wx.BoxSizer(wx.HORIZONTAL)
        cancel_limit_order_button = wx.Button(self, label='Cancel selected open order')
        cancel_limit_order_button.Bind(wx.EVT_BUTTON, self.main.on_cancel_limit_order)
        hsizer.Add(cancel_limit_order_button, 0, wx.ALL | wx.EXPAND, 5)

        cancel_all_limit_orders_button = wx.Button(self, label='Cancel all open orders')
        cancel_all_limit_orders_button.Bind(wx.EVT_BUTTON, self.main.on_cancel_all_limit_orders)
        hsizer.Add(cancel_all_limit_orders_button, 0, wx.ALL | wx.EXPAND, 5)
        main_sizer.Add(hsizer, 0, wx.ALL | wx.CENTER, 5)

        # open limit order
        hsizer = wx.BoxSizer(wx.HORIZONTAL)
        staticOpenOrderLimit = wx.StaticText(self,label = "Open order :", style = wx.ALIGN_LEFT)
        hsizer.Add(staticOpenOrderLimit,0, wx.ALL | wx.EXPAND, 5)
        self.symbols = wx.ComboBox(self,choices = ["BTC", "ETH", "XRP"])
        hsizer.Add(self.symbols,0, wx.ALL | wx.EXPAND, 5)
        staticAmount = wx.StaticText(self,label = "Amount ($)", style = wx.ALIGN_LEFT)
        hsizer.Add(staticAmount,0, wx.ALL | wx.EXPAND, 5)
        self.amount = FS.FloatSpin(self, -1, size=wx.Size(70, -1), min_val=0, increment=0.1, value=0., digits=2, agwStyle=FS.FS_LEFT)
        hsizer.Add(self.amount, 0, wx.ALL | wx.CENTER, 5)
        staticLeverage = wx.StaticText(self, label="Leverage", style=wx.ALIGN_LEFT)
        hsizer.Add(staticLeverage, 0, wx.ALL | wx.EXPAND, 5)
        self.leverage = FS.FloatSpin(self, -1, size=wx.Size(40, -1), min_val=1, increment=1, value=2, digits=0, agwStyle=FS.FS_LEFT)
        hsizer.Add(self.leverage, 0, wx.ALL | wx.CENTER, 5)
        staticPrice = wx.StaticText(self,label = "Price", style = wx.ALIGN_LEFT)
        hsizer.Add(staticPrice,0, wx.ALL | wx.EXPAND, 5)
        self.price = FS.FloatSpin(self, -1, size=wx.Size(70, -1), min_val=0, increment=0.1, value=0., digits=3, agwStyle=FS.FS_LEFT)
        hsizer.Add(self.price,0, wx.ALL | wx.EXPAND, 5)
        open_order_button = wx.Button(self, label="Open order")
        open_order_button.Bind(wx.EVT_BUTTON, self.main.on_open_limit_order)
        hsizer.Add(open_order_button, 0, wx.ALL | wx.CENTER, 5)
        main_sizer.Add(hsizer, 0, wx.ALL | wx.LEFT, 5)


#
class MainPanel(wx.Panel):

    def __init__(self, parent):
        super().__init__(parent)
        main_sizer = wx.BoxSizer(wx.VERTICAL)
        self.SetSizer(main_sizer)

        # Accounts
        staticTextAccounts = wx.StaticText(self,label="Accounts", style=wx.ALIGN_LEFT)
        main_sizer.Add(staticTextAccounts, 0, wx.ALL | wx.EXPAND, 5)

        hsizer_accounts = wx.BoxSizer(wx.HORIZONTAL)

        self.accounts = wx.ComboBox(self, choices=[])
        self.accounts.Bind(wx.EVT_COMBOBOX, self.on_account)
        hsizer_accounts.Add(self.accounts, 0, wx.ALL | wx.EXPAND, 5)

        button_account_update = wx.Button(self, label="Update")
        button_account_update.Bind(wx.EVT_BUTTON, self.on_account)
        hsizer_accounts.Add(button_account_update, 0, wx.ALL | wx.CENTER, 5)

        main_sizer.Add(hsizer_accounts, 0, wx.ALL | wx.LEFT, 5)

        # usdt equity
        self.staticTextUsdtEquity = wx.StaticText(self, label="USDT Equity : ", style=wx.ALIGN_LEFT)
        main_sizer.Add(self.staticTextUsdtEquity, 0, wx.ALL | wx.EXPAND, 5)

        sl1 = wx.StaticLine(self, size=(200, 1))
        main_sizer.Add(sl1, 0, wx.ALL | wx.EXPAND, 5)

        self.notebook_broker_state = wx.Notebook(self)
        self.panel_positions = PanelPositions(self.notebook_broker_state, self)
        self.notebook_broker_state.AddPage(self.panel_positions, "Positions")
        self.panel_orders = PanelOrders(self.notebook_broker_state, self)
        self.notebook_broker_state.AddPage(self.panel_orders, "Orders")
        main_sizer.Add(self.notebook_broker_state, 0, wx.ALL | wx.EXPAND, 5)

        sl3 = wx.StaticLine(self, size=(200, 1))
        main_sizer.Add(sl3, 0, wx.ALL | wx.EXPAND, 5)

        # Console
        staticTextConsole = wx.StaticText(self,label = "Console", style = wx.ALIGN_LEFT)
        main_sizer.Add(staticTextConsole,0, wx.ALL | wx.EXPAND, 5)

        self.log = wx.TextCtrl(self, -1, size=(200, 150), style=wx.TE_MULTILINE|wx.TE_READONLY|wx.HSCROLL)
        main_sizer.Add(self.log,0, wx.ALL | wx.EXPAND, 5)
        sys.stdout = RedirectText(self.log)

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
        if self.accounts.GetSelection() < 0:
            return None
        selected_account = self.accounts.GetString(self.accounts.GetSelection())
        account_info = settings_helper.get_account_info(selected_account)
        broker_name = account_info.get("broker", "")
        my_broker = None
        if broker_name == "bitget":
            my_broker = broker_bitget_api.BrokerBitGetApi(
                {"account": selected_account, "reset_account": "False"})
        return my_broker

    def update_usdt_equity(self, my_broker):
        usdt_equity = "-"
        if my_broker:
            usdt_equity = my_broker.get_usdt_equity()

        # update usdt equity
        print("usdt equity : ", usdt_equity)
        self.staticTextUsdtEquity.SetLabel("USDT Equity : "+utils.KeepNDecimals(usdt_equity))

    def update_positions(self, my_broker):
        positions = []
        available = 0
        if my_broker:
            positions = my_broker.get_open_position()

            available, crossMaxAvailable, fixedMaxAvailable = my_broker.get_available_cash()

        # update positions
        print("positions : ", positions)
        self.panel_positions.positions.DeleteAllItems()
        self.panel_positions.positions.Append(["USDT", utils.KeepNDecimals(available), "-", "-", "-"])
        if isinstance(positions, pd.DataFrame):
            for index, row in positions.iterrows():
                uPL = my_broker.get_symbol_unrealizedPL(row["symbol"])
                self.panel_positions.positions.Append([row["symbol"], utils.KeepNDecimals(row["available"]*row["marketPrice"]), row["holdSide"], row["leverage"], utils.KeepNDecimals(uPL)])

    def update_orders(self, my_broker):
        orders = []
        if my_broker:
            orders = my_broker.get_open_orders(["XRP"])

        # update orders
        print("orders : ", orders)
        self.panel_orders.orders.DeleteAllItems()
        if isinstance(orders, pd.DataFrame):
            for index, row in orders.iterrows():
                self.panel_orders.orders.Append([row["symbol"], row["side"], row["price"], row["leverage"], row["marginCoin"], row["clientOid"], row["orderId"]])

    def on_account(self, event):
        my_broker = self.get_broker_from_selected_account()
        if my_broker:
            self.update_usdt_equity(my_broker)
            self.update_positions(my_broker)
            self.update_orders(my_broker)

    def on_close_position(self, event):
        index = self.panel_positions.positions.GetFirstSelected()
        if index == -1:
            return
        symbol = self.panel_positions.positions.GetItem(index, col=0).GetText()
        gross_size = float(self.panel_positions.positions.GetItem(index, col=1).GetText())

        my_broker = self.get_broker_from_selected_account()
        if my_broker:
            mytrade = trade.Trade()
            mytrade.symbol = symbol
            mytrade.gross_size = gross_size
            mytrade.type = "CLOSE_LONG"
            print("close position : ", mytrade.symbol, " / ", mytrade.gross_size)
            my_broker.execute_trade(mytrade)
            self.update_positions(my_broker)

    def on_close_all_positions(self, event):
        pass

    def on_open_position(self, event):
        my_broker = self.get_broker_from_selected_account()
        if my_broker and self.panel_positions.symbols.GetSelection() >= 0:
            mytrade = trade.Trade()
            mytrade.symbol = self.panel_positions.symbols.GetString(self.panel_positions.symbols.GetSelection())
            mytrade.gross_size = self.panel_positions.amount.GetValue() / my_broker.get_value(mytrade.symbol)
            mytrade.type = "OPEN_LONG"
            print("open position : ", mytrade.symbol, " / ", mytrade.gross_size)
            my_broker.execute_trade(mytrade)
            self.update_positions(my_broker)

    def on_cancel_limit_order(self, event):
        index = self.panel_orders.orders.GetFirstSelected()
        if index == -1:
            return
        symbol = self.panel_orders.orders.GetItem(index, col=0).GetText()
        marginCoin = self.panel_orders.orders.GetItem(index, col=4).GetText()
        orderId = self.panel_orders.orders.GetItem(index, col=6).GetText()
        print("cancel open order : ", orderId)
        my_broker = self.get_broker_from_selected_account()
        my_broker.cancel_order(symbol, marginCoin, orderId)
        self.update_orders(my_broker)

    def on_cancel_all_limit_orders(self, event):
        my_broker = self.get_broker_from_selected_account()
        my_broker.cancel_all_orders(["XRP", "BTC", "ETH"])
        self.update_orders(my_broker)

    def on_open_limit_order(self, event):
        my_broker = self.get_broker_from_selected_account()
        if my_broker and self.panel_orders.symbols.GetSelection() >= 0:
            mytrade = trade.Trade()
            mytrade.symbol = self.panel_orders.symbols.GetString(self.panel_orders.symbols.GetSelection())
            mytrade.gross_size = self.panel_orders.amount.GetValue()
            mytrade.type = "OPEN_LONG_ORDER"
            mytrade.price = self.panel_orders.price.GetValue()
            print("open limit order : ", mytrade.symbol, " / ", mytrade.gross_size, " / ", mytrade.price)
            my_broker.execute_trade(mytrade)
            self.update_orders(my_broker)

#
class CragFrame(wx.Frame):

    def __init__(self):
        wx.Frame.__init__(self, parent=None, title='Crag UI',pos=wx.DefaultPosition,size=(700, 700), style= wx.SYSTEM_MENU | wx.CAPTION | wx.CLOSE_BOX)
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
