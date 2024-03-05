import pytest
import pandas as pd
from src import rtstr_grid_trading_long, rtdp
from src import crag, crag_helper
from src import broker
from . import test_rtstr, utils
import os

class BrokerMock(broker.Broker):

    def __init__(self, params=None):
        super().__init__(params)

    def get_info(self):
        return None, None, None

    def get_value(self, symbol):
        return 0.

    def get_commission(self, symbol):
        return 0.

    def execute_trade(self, trade):
        return ""

    def export_history(self, target):
        return ""

    def _get_symbol(self, coin):
        return ""

    def _get_coin(self, symbol):
        return ""


class TestRTSTRGridTradingLong:

    def test_constructor(self):
        # action
        strategy = rtstr_grid_trading_long.StrategyGridTradingLong()

        # expectations
        assert (strategy.SL == 0)
        assert (strategy.TP == 0)
        assert (strategy.MAX_POSITION == 5)
        assert (strategy.match_full_position == False)

    def test_get_data_description(self, mocker):
        # context
        strategy = rtstr_grid_trading_long.StrategyGridTradingLong()

        # action
        ds = strategy.get_data_description()

        # expectations
        assert (ds.symbols == rtdp.default_symbols)
        # assert(not set(ds.features) ^ set(['ao', 'n1_ao', 'ema_100', 'ema-200', 'stoch_rsi', 'willr']))
        assert (sorted(ds.features) == sorted(['ema_10']))

    def _initialize_current_data(self, strategy, data):
        ds = strategy.get_data_description()
        df_current_data = pd.DataFrame(data=data)
        df_current_data.set_index("symbol", inplace=True)
        strategy.set_current_data(df_current_data)
        return strategy

    def test_get_df_buying_symbols(self):
        # context
        strategy = rtstr_grid_trading_long.StrategyGridTradingLong()
        data = {"index": [0, 1], "symbol": ["BTC/USD", "ETH/USD"], "ema_10": [50, 50]}
        strategy = self._initialize_current_data(strategy, data)

        # action
        df = strategy.get_df_buying_symbols()

        # expectations
        assert (isinstance(df, pd.DataFrame))
        assert (any(item in df.columns.to_list() for item in ['symbol', 'stimulus', 'size', 'percent', 'gridzone', 'pos_type']))
        assert (len(df) == 0)

    def test_get_df_buying_symbols_with_rtctrl(self):
        # context
        strategy = rtstr_grid_trading_long.StrategyGridTradingLong()
        data = {"index": [0, 1], "symbol": ["BTC/USD", "ETH/USD"], "ema_10": [50, 50]}
        strategy = self._initialize_current_data(strategy, data)
        strategy.rtctrl.init_cash_value = 100
        test_rtstr.update_rtctrl(strategy)

        # action
        df = strategy.get_df_buying_symbols()

        # expectations
        assert (isinstance(df, pd.DataFrame))
        print(df.columns.to_list())
        assert (any(item in df.columns.to_list() for item in ['symbol', 'stimulus', 'size', 'percent', 'gridzone', 'pos_type']))
        assert (len(df) == 0)

    def test_get_df_selling_symbols(self):
        # context
        strategy = rtstr_grid_trading_long.StrategyGridTradingLong()
        lst_symbols = ["BTC/USD", "ETH/USD"]
        data = {"index": [0, 1], "symbol": lst_symbols, "ema_10": [50, 50]}
        strategy = self._initialize_current_data(strategy, data)

        # action
        df = strategy.get_df_selling_symbols(lst_symbols, None)

        # expectations
        assert (isinstance(df, pd.DataFrame))
        assert (any(item in df.columns.to_list() for item in ['symbol', 'stimulus']))
        assert (len(df) == 0)

    def test_crag_run(self):
        # context

        # create the configruation file
        filename = utils.write_file("./test/generated/crag.xml", '''<configuration>
            <strategy name="StrategyGridTradingLong">
                <params symbols="BTC/USD" grid_df_params="./test/data/multigrid_df_params.csv"/>
            </strategy>
            <broker name="mock">
                <params exchange="broker" account="test_bot" simulation="1" reset_account="False" />
            </broker>
            <crag interval="20" />
        </configuration>''')


        configuration = crag_helper.load_configuration_file("../"+filename)
        params = crag_helper.get_crag_params_from_configuration(configuration)

        params["broker"] = BrokerMock()

        bot = crag.Crag(params)


        # action
        bot.run()

        # expectations
        #assert (isinstance(df, pd.DataFrame))
        #assert (any(item in df.columns.to_list() for item in ['symbol', 'stimulus']))
        #assert (len(df) == 0)

        # cleaning
        os.remove(filename)

