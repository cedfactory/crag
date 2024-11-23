import pytest
from unittest.mock import patch
from src import logger,rtstr_grid_trading_generic_v3

class TestGridPosition:

    def test_constructor(self):
        # action
        grid = rtstr_grid_trading_generic_v3.GridPosition("strategy_name", "long", "XRP", 0.55, 0.45, 60,
                                                          10.,  # percent_per_grid
                                                          5,  # nb_position_limits
                                                          "strategy_id",  # strategy_id
                                                          )

        # expectations
        assert(grid.nb_position_limits == 5)
        # todo : to complete
