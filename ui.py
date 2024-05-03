import pandas as pd
import wx
import wx.lib.agw.floatspin as FS
import sys

from src.utils import settings_helper
from src import broker_helper,broker_bitget_api,utils,trade



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
        self.positions.InsertColumn(0, 'Symbol', width=130)
        self.positions.InsertColumn(1, 'USDT Equity', width=100)
        self.positions.InsertColumn(2, 'Side', width=50)
        self.positions.InsertColumn(3, 'Leverage', width=70)
        self.positions.InsertColumn(4, 'uPL', width=70)
        self.positions.InsertColumn(5, 'Total', width=70)
        self.positions.InsertColumn(6, 'Value', width=70)
        self.positions.InsertColumn(7, 'Avg Open Price', width=70)
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
        staticOpenPosition = wx.StaticText(self,label = "Open position :", style=wx.ALIGN_LEFT)
        hsizer.Add(staticOpenPosition,0, wx.ALL | wx.ALIGN_CENTER, 5)
        self.symbols = wx.ComboBox(self,choices = ["BTC", "ETH", "XRP",  "SOL"])
        hsizer.Add(self.symbols,0, wx.ALL | wx.CENTER, 5)

        panel = wx.Panel(self, -1)
        self.rb_amount = wx.RadioButton(panel, -1, 'Amount ($)', (10, 10), style=wx.RB_GROUP)
        self.rb_size = wx.RadioButton(panel, -1, 'Size ($)', (10, 30))
        hsizer.Add(panel, 0, wx.ALL | wx.CENTER, 5)

        self.amount = FS.FloatSpin(self, -1, size=wx.Size(70, -1), min_val=0, increment=0.1, value=0., digits=3, agwStyle=FS.FS_LEFT)
        hsizer.Add(self.amount, 0, wx.ALL | wx.CENTER, 5)
        staticLeverage = wx.StaticText(self, label="Leverage", style=wx.ALIGN_LEFT)
        hsizer.Add(staticLeverage, 0, wx.ALL | wx.CENTER, 5)
        self.leverage = FS.FloatSpin(self, -1, size=wx.Size(40, -1), min_val=1, increment=1, value=2, digits=0, agwStyle=FS.FS_LEFT)
        hsizer.Add(self.leverage, 0, wx.ALL | wx.CENTER, 5)
        self.sides = wx.ComboBox(self, choices=["long", "short"])
        self.sides.SetSelection(0)
        hsizer.Add(self.sides, 0, wx.ALL | wx.CENTER, 5)
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
        self.orders.InsertColumn(3, 'Size', width=70)
        self.orders.InsertColumn(4, 'Leverage', width=70)
        self.orders.InsertColumn(5, 'MarginCoin', width=70)
        self.orders.InsertColumn(6, 'ClientOid', width=130)
        self.orders.InsertColumn(7, 'OrderId', width=130)
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
        hsizer.Add(staticOpenOrderLimit,0, wx.ALL | wx.CENTER, 5)
        self.symbols = wx.ComboBox(self,choices = ["BTC", "ETH", "XRP", "SOL"])
        hsizer.Add(self.symbols,0, wx.ALL | wx.CENTER, 5)

        panel = wx.Panel(self, -1)
        self.rb_amount = wx.RadioButton(panel, -1, 'Amount ($)', (10, 10), style=wx.RB_GROUP)
        self.rb_size = wx.RadioButton(panel, -1, 'Size ($)', (10, 30))
        hsizer.Add(panel, 0, wx.ALL | wx.CENTER, 5)

        self.amount = FS.FloatSpin(self, -1, size=wx.Size(60, -1), min_val=0, increment=0.1, value=0., digits=2, agwStyle=FS.FS_LEFT)
        hsizer.Add(self.amount, 0, wx.ALL | wx.CENTER, 5)
        staticLeverage = wx.StaticText(self, label="Leverage", style=wx.ALIGN_LEFT)
        hsizer.Add(staticLeverage, 0, wx.ALL | wx.CENTER, 5)
        self.leverage = FS.FloatSpin(self, -1, size=wx.Size(40, -1), min_val=1, increment=1, value=2, digits=0, agwStyle=FS.FS_LEFT)
        hsizer.Add(self.leverage, 0, wx.ALL | wx.CENTER, 5)
        staticPrice = wx.StaticText(self, label="Price", style = wx.ALIGN_LEFT)
        hsizer.Add(staticPrice,0, wx.ALL | wx.CENTER, 5)
        self.price = FS.FloatSpin(self, -1, size=wx.Size(60, -1), min_val=0, increment=0.1, value=0., digits=3, agwStyle=FS.FS_LEFT)
        hsizer.Add(self.price,0, wx.ALL | wx.CENTER, 5)
        self.sides = wx.ComboBox(self, choices=["long", "short"])
        self.sides.SetSelection(0)
        hsizer.Add(self.sides, 0, wx.ALL | wx.CENTER, 5)
        open_order_button = wx.Button(self, label="Open order")
        open_order_button.Bind(wx.EVT_BUTTON, self.main.on_open_limit_order)
        hsizer.Add(open_order_button, 0, wx.ALL | wx.CENTER, 5)
        main_sizer.Add(hsizer, 0, wx.ALL | wx.LEFT, 5)


class PanelTriggers(wx.Panel):
    def __init__(self, parent, main):
        wx.Panel.__init__(self, parent)
        main_sizer = wx.BoxSizer(wx.VERTICAL)
        self.SetSizer(main_sizer)

        self.main = main

        self.triggers = wx.ListCtrl(
            self, size=(570, 200),
            style=wx.LC_REPORT | wx.BORDER_SUNKEN
        )
        self.triggers.InsertColumn(0, 'PlanType', width=70)
        self.triggers.InsertColumn(1, 'Symbol', width=70)
        self.triggers.InsertColumn(2, 'Size', width=40)
        self.triggers.InsertColumn(3, 'Side', width=40)
        self.triggers.InsertColumn(4, 'OderType', width=60)
        self.triggers.InsertColumn(5, 'Price', width=40)
        self.triggers.InsertColumn(6, 'TriggerPrice', width=70)
        self.triggers.InsertColumn(7, 'TriggerType', width=70)
        self.triggers.InsertColumn(8, 'MarginMode', width=90)
        self.triggers.InsertColumn(9, 'ClientOid', width=120)
        main_sizer.Add(self.triggers, 0, wx.ALL | wx.EXPAND, 5)

        hsizer = wx.BoxSizer(wx.HORIZONTAL)
        cancel_trigger_button = wx.Button(self, label='Cancel selected trigger')
        cancel_trigger_button.Bind(wx.EVT_BUTTON, self.main.on_cancel_trigger)
        hsizer.Add(cancel_trigger_button, 0, wx.ALL | wx.EXPAND, 5)

        cancel_all_triggers_button = wx.Button(self, label='Cancel all triggers')
        cancel_all_triggers_button.Bind(wx.EVT_BUTTON, self.main.on_cancel_all_triggers)
        hsizer.Add(cancel_all_triggers_button, 0, wx.ALL | wx.EXPAND, 5)
        main_sizer.Add(hsizer, 0, wx.ALL | wx.CENTER, 5)


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

        button_account_all_accounts = wx.Button(self, label="All accounts")
        button_account_all_accounts.Bind(wx.EVT_BUTTON, self.on_all_accounts)

        main_sizer.Add(button_account_all_accounts, 0, wx.ALL | wx.LEFT, 5)

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
        self.panel_triggers = PanelTriggers(self.notebook_broker_state, self)
        self.notebook_broker_state.AddPage(self.panel_triggers, "Triggers")
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
                total = row["total"]
                value = row["total"] * row["marketPrice"]
                avgOpenPrice = row["averageOpenPrice"]
                self.panel_positions.positions.Append([row["symbol"], utils.KeepNDecimals(row["total"]*row["marketPrice"]), row["holdSide"], row["leverage"], utils.KeepNDecimals(uPL), utils.KeepNDecimals(total), utils.KeepNDecimals(value), utils.KeepNDecimals(avgOpenPrice)])

    def update_orders(self, my_broker):
        orders = []
        if my_broker:
            orders = my_broker.get_open_orders(["XRP", "BTC", "ETH", "SOL"])

        # update orders
        print("orders : ", orders)
        self.panel_orders.orders.DeleteAllItems()
        if isinstance(orders, pd.DataFrame):
            for index, row in orders.iterrows():
                self.panel_orders.orders.Append([row["symbol"], row["side"], row["price"], row["size"], row["leverage"], row["marginCoin"], row["clientOid"], row["orderId"]])

    def update_triggers(self, my_broker):
        triggers = []
        if my_broker:
            triggers = my_broker.get_triggers()

        # update orders
        print("triggers : ", triggers)
        self.panel_triggers.triggers.DeleteAllItems()
        if isinstance(triggers, pd.DataFrame):
            for index, row in triggers.iterrows():
                self.panel_triggers.triggers.Append([row["planType"], row["symbol"], row["size"], row["side"], row["orderType"], row["price"], row["triggerPrice"], row["triggerType"], row["marginMode"], row["clientOid"]])

    def on_account(self, event):
        my_broker = self.get_broker_from_selected_account()
        if my_broker:
            self.update_usdt_equity(my_broker)
            self.update_positions(my_broker)
            self.update_orders(my_broker)
            self.update_triggers(my_broker)

    def on_all_accounts(self, event):
        df = broker_helper.get_usdt_equity_all_accounts()
        print(df)
        print(utils.KeepNDecimals(df["USDT_Equity"].sum()))

    def on_close_position(self, event):
        index = self.panel_positions.positions.GetFirstSelected()
        if index == -1:
            return
        symbol = self.panel_positions.positions.GetItem(index, col=0).GetText()
        gross_size = float(self.panel_positions.positions.GetItem(index, col=1).GetText())
        side = self.panel_positions.positions.GetItem(index, col=2).GetText()

        my_broker = self.get_broker_from_selected_account()
        if my_broker:
            mytrade = trade.Trade()
            mytrade.symbol = symbol
            mytrade.gross_size = gross_size
            if side == "long":
                mytrade.type = "CLOSE_LONG"
            else:
                mytrade.type = "CLOSE_SHORT"
            print("close position : ", mytrade.symbol, " / ", mytrade.gross_size)
            my_broker.execute_trade(mytrade)
            self.update_positions(my_broker)

    def on_close_all_positions(self, event):
        my_broker = self.get_broker_from_selected_account()
        if my_broker:
            my_broker.execute_reset_account()

    def on_open_position(self, event):
        my_broker = self.get_broker_from_selected_account()
        if my_broker and self.panel_positions.symbols.GetSelection() >= 0:
            symbol = self.panel_positions.symbols.GetString(self.panel_positions.symbols.GetSelection())
            leverage = self.panel_positions.leverage.GetValue()
            side = self.panel_positions.sides.GetString(self.panel_positions.sides.GetSelection()) # "long" or "short"
            my_broker.set_symbol_leverage(my_broker._get_symbol(symbol), int(leverage), side)

            mytrade = trade.Trade()
            mytrade.symbol = symbol
            if self.panel_positions.rb_amount.GetValue(): # amount
                mytrade.gross_size = self.panel_positions.amount.GetValue() / my_broker.get_value(mytrade.symbol)
            elif self.panel_positions.rb_size.GetValue():
                mytrade.gross_size = self.panel_positions.amount.GetValue()
            else:
                print("open position : amount or size ???")
                return
            if side == "long":
                mytrade.type = "OPEN_LONG"
            else:
                mytrade.type = "OPEN_SHORT"
            print("open position : ", mytrade.symbol, " / ", mytrade.gross_size)
            my_broker.execute_trade(mytrade)
            self.update_positions(my_broker)

    def on_cancel_limit_order(self, event):
        index = self.panel_orders.orders.GetFirstSelected()
        if index == -1:
            return
        symbol = self.panel_orders.orders.GetItem(index, col=0).GetText()
        marginCoin = self.panel_orders.orders.GetItem(index, col=5).GetText()
        orderId = self.panel_orders.orders.GetItem(index, col=7).GetText()
        print("cancel open order : ", orderId)
        my_broker = self.get_broker_from_selected_account()
        my_broker.cancel_order(symbol, marginCoin, orderId)
        self.update_orders(my_broker)

    def on_cancel_all_limit_orders(self, event):
        my_broker = self.get_broker_from_selected_account()
        my_broker.cancel_all_orders(["XRP", "BTC", "ETH", "SOL"])
        self.update_orders(my_broker)

    def on_open_limit_order(self, event):
        my_broker = self.get_broker_from_selected_account()
        if my_broker and self.panel_orders.symbols.GetSelection() >= 0:
            symbol = self.panel_orders.symbols.GetString(self.panel_orders.symbols.GetSelection())
            leverage = self.panel_orders.leverage.GetValue()
            side = self.panel_orders.sides.GetString(self.panel_orders.sides.GetSelection()) # "long" or "short"
            my_broker.set_symbol_leverage(my_broker._get_symbol(symbol), int(leverage), side)

            mytrade = trade.Trade()
            mytrade.symbol = symbol
            if self.panel_orders.rb_amount.GetValue(): # amount
                mytrade.gross_size = self.panel_orders.amount.GetValue() / my_broker.get_value(mytrade.symbol)
            elif self.panel_orders.rb_size.GetValue():
                mytrade.gross_size = self.panel_orders.amount.GetValue()
            else:
                print("open limit order : amount or size ???")
                return
            if side == "long":
                mytrade.type = "OPEN_LONG_ORDER"
            else:
                mytrade.type = "OPEN_SHORT_ORDER"
            mytrade.price = self.panel_orders.price.GetValue()
            print("open limit order : ", mytrade.symbol, " / ", mytrade.gross_size, " / ", mytrade.price)
            my_broker.execute_trade(mytrade)
            self.update_orders(my_broker)



    def on_cancel_trigger(self, event):
        index = self.panel_triggers.triggers.GetFirstSelected()
        if index == -1:
            return
        symbol = self.panel_triggers.triggers.GetItem(index, col=1).GetText()

        my_broker = self.get_broker_from_selected_account()
        #my_broker.cancel_order(symbol, marginCoin, orderId)
        self.update_triggers(my_broker)

    def on_cancel_all_triggers(self, event):
        my_broker = self.get_broker_from_selected_account()
        self.update_triggers(my_broker)

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
