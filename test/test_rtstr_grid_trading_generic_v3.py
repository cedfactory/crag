import pytest
from unittest.mock import patch
import pandas as pd
from src import rtstr_grid_trading_generic_v3, rtdp
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

    def get_current_data(self, data_description):
        return {}

class TestRTSTRGridTradingLong:

    def test_constructor(self):
        # action
        strategy = rtstr_grid_trading_generic_v3.StrategyGridTradingGenericV3(params={"id": "grid1", "grid_low":"1.", "grid_high":"5.", "nb_grid":"5", "type":"long"})

        # expectations
        assert (strategy.SL == 0)
        assert (strategy.TP == 0)
        assert (strategy.MAX_POSITION == 1)
        assert (strategy.match_full_position is False)

    def test_get_data_description(self, mocker):
        # context
        strategy = rtstr_grid_trading_generic_v3.StrategyGridTradingGenericV3(params={"id": "grid1", "grid_low":"1.", "grid_high":"5.", "nb_grid":"5", "type":"long"})

        # action
        ds = strategy.get_data_description()

        # expectations
        assert (len(ds) == 0)

    def _initialize_current_data(self, strategy, data):
        ds = strategy.get_data_description()
        df_current_data = pd.DataFrame(data=data)
        df_current_data.set_index("symbol", inplace=True)
        strategy.set_current_data(df_current_data)
        return strategy

    def test_get_df_buying_symbols(self):
        # context
        strategy = rtstr_grid_trading_generic_v3.StrategyGridTradingGenericV3(params={"id": "grid1", "grid_low":"1.", "grid_high":"5.", "nb_grid":"5", "type":"long"})
        data = {"index": [0, 1], "symbol": ["BTC/USD", "ETH/USD"], "ema_10": [50, 50]}
        strategy = self._initialize_current_data(strategy, data)

        # action
        df = strategy.get_df_buying_symbols()

        # expectations
        assert (isinstance(df, pd.DataFrame))
        assert (any(item in df.columns.to_list() for item in ['symbol', 'stimulus', 'size', 'percent', 'gridzone', 'pos_type']))
        assert (len(df) == 0)

    def test_crag_run(self, mocker):
        return
        # context
        def mock_step(self):
            return False

        mocker.patch('src.crag.Crag.step',mock_step)

        # create the configruation file
        filename = utils.write_file("./test/generated/crag.xml", '''<configuration>
            <strategy name="StrategyDummyTest">
                <params symbols="BTC/USD" grid_df_params="./test/data/multigrid_df_params.csv"
                sl="0" tp="0" global_sl="0" global_tp="0" global_safety_TP="0" global_safety_SL="0"
                grid_high="130" grid_low="100" percent_per_grid="0.4" nb_grid="5" grid_margin="500"/>
            </strategy>
            <broker name="mock">
                <params exchange="broker" account="test_bot" reset_account_start="False" />
            </broker>
            <crag interval="20" />
        </configuration>''')


        configuration = crag_helper.load_configuration_file("../"+filename)
        params = crag_helper.get_crag_params_from_configuration(configuration)

        params["broker"] = BrokerMock()

        params["rtstr"].global_safety_TP = 0
        params["rtstr"].global_safety_SL = 0

        bot = crag.Crag(params)


        # action
        bot.run()

        # expectations
        #assert (isinstance(df, pd.DataFrame))
        #assert (any(item in df.columns.to_list() for item in ['symbol', 'stimulus']))
        #assert (len(df) == 0)

        # cleaning
        os.remove(filename)

