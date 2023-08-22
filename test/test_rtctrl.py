import pytest
from src import trade,rtctrl

def get_current_trades_sample():
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
        current_trade.net_size = content["size"]
        current_trade.buying_fee = content["buying_fee"]
        current_trade.gross_price = content["gross_price"]
        current_trades.append(current_trade)

    return current_trades

class TestRTCTRL:

    def test_get_list_of_traded_symbols(self):
        # context
        current_trades = get_current_trades_sample()
        ctrl = rtctrl.rtctrl()
        ctrl.set_list_open_position_type(["BUY"])
        ctrl.set_list_close_position_type(["SELL"])

        # action
        lst_symbols = ctrl.get_list_of_traded_symbols(current_trades)

        # expectations
        expected_lst_symbols = ['symbol2', 'symbol3']
        assert(len(lst_symbols) == len(set(lst_symbols)))
        assert(not set(lst_symbols) ^ set(expected_lst_symbols))

    def test_get_list_of_asset_size(self):
        # context
        current_trades = get_current_trades_sample()
        ctrl = rtctrl.rtctrl()
        ctrl.symbols = ['symbol2', 'symbol3']
        ctrl.set_list_open_position_type(["BUY"])
        ctrl.set_list_close_position_type(["SELL"])

        # action
        lst_of_asset_size = ctrl.get_list_of_asset_size(current_trades)

        # expectations
        expected_lst_of_asset_size = [5, 4]
        assert(lst_of_asset_size == expected_lst_of_asset_size)

    def test_get_list_of_asset_fees(self):
        # context
        current_trades = get_current_trades_sample()
        ctrl = rtctrl.rtctrl()
        ctrl.symbols = ['symbol2', 'symbol3']
        ctrl.set_list_open_position_type(["BUY"])
        ctrl.set_list_close_position_type(["SELL"])

        # action
        lst_of_asset_size = ctrl.get_list_of_asset_fees(current_trades)

        # expectations
        expected_lst_of_asset_size = [.5, .4]
        assert(lst_of_asset_size == expected_lst_of_asset_size)

    def test_get_list_of_asset_gross_price(self):
        # context
        current_trades = get_current_trades_sample()
        ctrl = rtctrl.rtctrl()
        ctrl.symbols = ['symbol2', 'symbol3']
        ctrl.set_list_open_position_type(["BUY"])
        ctrl.set_list_close_position_type(["SELL"])

        # action
        lst_of_asset_size = ctrl.get_list_of_asset_gross_price(current_trades)

        # expectations
        expected_lst_of_asset_size = [50, 40]
        assert(lst_of_asset_size == expected_lst_of_asset_size)

    def test_update(self, mocker):
        # context
        current_trades = get_current_trades_sample()
        ctrl = rtctrl.rtctrl()
        ctrl.set_list_open_position_type(["BUY"])
        ctrl.set_list_close_position_type(["SELL"])

        # action
        prices_symbols = {'symbol1': 0.01, 'symbol2': 0.02, 'symbol3': 0.03, 'symbol4': 0.04}
        current_datetime = "2022-04-01"
        ctrl.update_rtctrl(current_datetime, current_trades, 100, 100, prices_symbols, None, None)

        # expectations
        df = ctrl.df_rtctrl
        df.set_index("symbol", inplace=True)
        df.sort_index(inplace=True) # be sure the order of the rows if "symbol2", "symbol3"
        assert(df['size'].to_list() == [5, 4])
        assert(df['fees'].to_list()== [.5, .4])
        assert(df['buying_gross_price'].to_list() == [50, 40])
        assert(df['portfolio_value'].to_list() == [0.1, 0.12])
        assert(ctrl.wallet_cash == 100)
        
