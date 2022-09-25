import pytest
import pandas as pd
from rich import print
from src import rtstr_grid_trading

class TestGridLevelPosition:
    
    def test_constructor(self):
        # context
        params = {"grid_step": 25.}

        # action
        gridLevel = rtstr_grid_trading.GridLevelPosition(params=params)
        
        # expectations
        assert(gridLevel.UpperPriceLimit == 25000)
        assert(gridLevel.LowerPriceLimit == 15000)
        expected_data = {
            "zone_id":["zone_0", "zone_1", "zone_2", "zone_3", "zone_4", "zone_5"],
            "start":[25000, 22500, 20000, 17500, 15000, 10000],
            "end": [100000, 25000, 22500, 20000, 17500, 15000],
            "previous_position": [0, 0, 0, 0, 0, 0],
            "actual_position": [0, 0, 0, 0, 0, 0],
            "zone_engaged": [False, False, False, False, False, False],
            "buying_value": [0, 0, 0, 0, 0, 0]
        }
        df_expected = pd.DataFrame(expected_data)
        pd.testing.assert_frame_equal(gridLevel.df_grid, df_expected)
        assert(gridLevel.grid_size == 6)

    def test_get_zone_position(self):
         # context
        params = {"grid_step": 25.}
        gridLevel = rtstr_grid_trading.GridLevelPosition(params=params)

        # action
        zone_position = gridLevel.get_zone_position(15550)

        # expectations
        assert(zone_position == 4)

    def test_get_set_previous_zone_position(self):
         # context
        params = {"grid_step": 25.}
        gridLevel = rtstr_grid_trading.GridLevelPosition(params=params)
        previous_zone_position = gridLevel.get_previous_zone_position()
        assert(previous_zone_position == -1)

        # action 1
        gridLevel.set_previous_zone_position(15550)

        # expectations 1
        previous_zone_position = gridLevel.get_previous_zone_position()
        assert(previous_zone_position == 4)

        # action 2
        gridLevel.set_previous_zone_position(20500)

        # expectations 2
        previous_zone_position = gridLevel.get_previous_zone_position()
        assert(previous_zone_position == 2)

class TestRTSTRGridTrading:

    def ttest_constructor(self):
        # action
        strategy = rtstr_grid_trading.StrategyGridTrading()

        # expectations
        assert(strategy.SL == -1000)
        assert(strategy.TP == 1000)
        assert(strategy.MAX_POSITION == 5)
        assert(strategy.SPLIT == 5)
        assert(strategy.match_full_position == True)
