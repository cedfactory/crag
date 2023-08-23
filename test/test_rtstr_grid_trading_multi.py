import pytest
import pandas as pd
from rich import print
from src import rtstr_grid_trading_multi

class TestGridLevelPosition:
    
    def test_constructor(self):
        # context
        symbol = "BTC/USD"
        params = {"symbols": symbol, "grid_df_params": "../test/data/multigrid_df_params.csv"}

        # action
        gridLevel = rtstr_grid_trading_multi.GridLevelPosition(symbol, params)
        
        # expectations
        assert(gridLevel.UpperPriceLimit == 21100)
        assert(gridLevel.LowerPriceLimit == 19900)
        expected_data = {
            "zone_id": ["zone_0", "zone_1", "zone_2", "zone_3", "zone_4", "zone_5", "zone_6", "zone_7", "zone_8", "zone_9", "zone_10", "zone_11"],
            "start": [21116.880, 20996.784, 20876.688, 20756.592, 20636.496, 20516.400, 20396.304, 20276.208, 20156.112, 20036.016, 19915.920, 10008.000],
            "end": [99920.000, 21083.120, 20963.216, 20843.312, 20723.408, 20603.504, 20483.600, 20363.696, 20243.792, 20123.888, 20003.984, 19884.080],
            "previous_position": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            "actual_position": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            "zone_engaged": [False, False, False, False, False, False, False, False, False, False, False, False],
            "buying_value": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            "flushed": [False, False, False, False, False, False, False, False, False, False, False, False]
        }
        df_expected = pd.DataFrame(expected_data)
        pd.testing.assert_frame_equal(gridLevel.df_grid, df_expected)
        assert(gridLevel.grid_size == 12)

    def test_get_zone_position(self):
         # context
        symbol = "BTC/USD"
        params = {"symbols": symbol, "grid_df_params": "../test/data/multigrid_df_params.csv"}
        gridLevel = rtstr_grid_trading_multi.GridLevelPosition(symbol, params)

        # action
        zone_position = gridLevel.get_zone_position(19999)

        # expectations
        assert(zone_position == 10)

    def test_get_set_previous_zone_position(self):
         # context
        symbol = "BTC/USD"
        params = {"symbols": symbol, "grid_df_params": "../test/data/multigrid_df_params.csv"}
        gridLevel = rtstr_grid_trading_multi.GridLevelPosition(symbol, params)
        previous_zone_position = gridLevel.get_previous_zone_position()
        assert(previous_zone_position == -1)

        # action 1
        gridLevel.set_previous_zone_position(19999)

        # expectations 1
        previous_zone_position = gridLevel.get_previous_zone_position()
        assert(previous_zone_position == 10)

        # action 2
        gridLevel.set_previous_zone_position(20450)

        # expectations 2
        previous_zone_position = gridLevel.get_previous_zone_position()
        assert(previous_zone_position == 6)

class TestRTSTRGridMultiTrading:

    def test_constructor(self):
        # context
        params = {"symbols": "BTC/USD", "grid_df_params": "../test/data/multigrid_df_params.csv"}

        # action
        strategy = rtstr_grid_trading_multi.StrategyGridTradingMulti(params)

        # expectations
        assert(strategy.share_size == 10)
        assert(strategy.SL == 0)
        assert(strategy.TP == 0)
        assert(strategy.MAX_POSITION == 5)
        assert(strategy.match_full_position == False)

    def test_get_data_description(self):
        # context
        params = {"symbols": "BTC/USD", "grid_df_params": "../test/data/multigrid_df_params.csv"}
        strategy = rtstr_grid_trading_multi.StrategyGridTradingMulti(params)

        # action
        ds = strategy.get_data_description()

        # expectations
        assert(ds.symbols == ["BTC/USD"])
        assert(not set(list(ds.features.keys())) ^ set(['close', 'slope_30', 'sma_30']))

    def _initialize_current_data(self, strategy, data):
        strategy.get_data_description()
        df_current_data = pd.DataFrame(data=data)
        df_current_data.set_index("symbol", inplace=True)
        strategy.set_current_data(df_current_data)
        return strategy

    def test_get_df_buying_symbols(self):
        # context
        params = {"symbols": "BTC/USD", "grid_df_params": "../test/data/multigrid_df_params.csv"}
        strategy = rtstr_grid_trading_multi.StrategyGridTradingMulti(params)
        data = {"index":[0], "symbol":["BTC/USD"], "close":[19000.]}
        strategy = self._initialize_current_data(strategy, data)
        strategy.set_df_multi()
        strategy.df_grid_multi.loc[strategy.df_grid_multi['symbol'] == "BTC/USD", "grid"].iloc[0].set_previous_zone_position(20000.)

        # action
        df = strategy.get_df_buying_symbols()

        # expectations
        assert(isinstance(df, pd.DataFrame))
        assert(any(item in df.columns.to_list() for item in ['symbol', 'size', 'percent']))
        assert(len(df) == 1)
        assert(df.iloc[0]['symbol'] == "BTC/USD")
        assert(df.iloc[0]['size'] == 0)
        assert(df.iloc[0]['percent'] == 0)
        assert(df.iloc[0]['gridzone'] == 0)

    def test_get_df_selling_symbols(self):
        return  # TODO : reactivate the test

        # context
        params = {"symbols": "BTC/USD", "grid_df_params": "../test/data/multigrid_df_params.csv"}
        strategy = rtstr_grid_trading_multi.StrategyGridTradingMulti(params)
        data = {"index":[0], "symbol":["BTC/USD"], "close":[20000.]}
        strategy = self._initialize_current_data(strategy, data)
        strategy.set_df_multi()
        strategy.df_grid_multi.loc[strategy.df_grid_multi['symbol'] == "BTC/USD", "grid"].iloc[0].set_previous_zone_position(19000.)
        # set init_cash_value
        strategy.rtctrl.update_rtctrl("not_final_time", [], 100, 100, [], "final_time", None)
        # set engaged level zone
        strategy.set_zone_engaged("BTC/USD", 19000.)

        # action
        df = strategy.get_df_selling_symbols(["BTC/USD"], None)

        # expectations
        assert(isinstance(df, pd.DataFrame))
        assert(any(item in df.columns.to_list() for item in ['symbol', 'stimulus']))
        assert(len(df) == 1)
        assert(df.iloc[0]['symbol'] == "BTC/USD")
        assert(df.iloc[0]['stimulus'] == "SELL")
 
    def test_get_symbol_buying_size(self):
        return  # TODO : reactivate the test

        # context
        params = {"symbols": "BTC/USD", "grid_df_params": "../test/data/multigrid_df_params.csv"}
        strategy = rtstr_grid_trading_multi.StrategyGridTradingMulti(params)
        data = {"index":[0], "symbol":["BTC/USD"], "close":[20000.]}
        strategy = self._initialize_current_data(strategy, data)
        strategy.set_df_multi()
        strategy.df_grid_multi.loc[strategy.df_grid_multi['symbol'] == "BTC/USD", "grid"].iloc[0].set_previous_zone_position(19000.)
        prices_symbols = {"BTC/USD": 0.01}
        strategy.rtctrl.update_rtctrl("not_final_time", [], 100, 100, prices_symbols, "final_time", None)
        # set engaged level zone
        strategy.set_zone_engaged("BTC/USD", 19000.)

        #action
        size, percent, zone_position = strategy.get_symbol_buying_size("BTC/USD")
        
        # expectations
        assert(size == pytest.approx(1000., abs=1e-6))
        assert(percent == pytest.approx(10., abs=1e-6))
        assert(zone_position == -1)
