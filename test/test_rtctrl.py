import pytest
from src import trade,rtctrl

class TestRTCTRL:

    def get_current_trades_for_get_functions(self):
        current_trades_content = [
            {'type':'SELL', 'symbol':'symbol1', 'size':1, 'buying_fee':0.1, 'gross_price':10},
            {'type':'BUY', 'symbol':'symbol2', 'size':2, 'buying_fee':0.2, 'gross_price':20},
            {'type':'BUY', 'symbol':'symbol2', 'size':3, 'buying_fee':0.3, 'gross_price':30},
            {'type':'BUY', 'symbol':'symbol3', 'size':4, 'buying_fee':0.4, 'gross_price':40}
            ]
        current_trades = []
        for content in current_trades_content:
            current_trade = trade.Trade()
            current_trade.type = content["type"]
            current_trade.symbol = content["symbol"]
            current_trade.size = content["size"]
            current_trade.buying_fee = content["buying_fee"]
            current_trade.gross_price = content["gross_price"]
            current_trades.append(current_trade)
        
        return current_trades


    def test_get_list_of_traded_symbols(self):
        # context
        current_trades = self.get_current_trades_for_get_functions()
        ctrl = rtctrl.rtctrl()

        # action
        lst_symbols = ctrl.get_list_of_traded_symbols(current_trades)

        # expectations
        expected_lst_symbols = ['symbol2', 'symbol3']
        assert(len(lst_symbols) == len(set(lst_symbols)))
        assert(not set(lst_symbols) ^ set(expected_lst_symbols))

    def test_get_list_of_asset_size(self):
        # context
        current_trades = self.get_current_trades_for_get_functions()
        ctrl = rtctrl.rtctrl()
        ctrl.symbol = ['symbol2', 'symbol3']

        # action
        lst_of_asset_size = ctrl.get_list_of_asset_size(current_trades)

        # expectations
        expected_lst_of_asset_size = [5, 4]
        assert(lst_of_asset_size == expected_lst_of_asset_size)

    def test_get_list_of_asset_fees(self):
        # context
        current_trades = self.get_current_trades_for_get_functions()
        ctrl = rtctrl.rtctrl()
        ctrl.symbol = ['symbol2', 'symbol3']

        # action
        lst_of_asset_size = ctrl.get_list_of_asset_fees(current_trades)

        # expectations
        expected_lst_of_asset_size = [.5, .4]
        assert(lst_of_asset_size == expected_lst_of_asset_size)

    def test_get_list_of_asset_gross_price(self):
        # context
        current_trades = self.get_current_trades_for_get_functions()
        ctrl = rtctrl.rtctrl()
        ctrl.symbol = ['symbol2', 'symbol3']

        # action
        lst_of_asset_size = ctrl.get_list_of_asset_gross_price(current_trades)

        # expectations
        expected_lst_of_asset_size = [50, 40]
        assert(lst_of_asset_size == expected_lst_of_asset_size)