import pytest
from unittest.mock import patch
import pandas as pd
from src import logger,rtstr_grid_trading_long
import gc
import os
import sys

class TestGridPosition:

    def test_constructor(self):
        # action
        grid = rtstr_grid_trading_long.GridPosition(["XRP"], 0.55, 0.45, 60, 0., False)

        # expectations
        assert(len(grid.grid['XRP'].index) == 61)

    def test_get_nb_open_positions_from_state(self):
        # context
        grid = rtstr_grid_trading_long.GridPosition(["XRP"], 0.55, 0.45, 60, 0., False)
        my_logger = logger.LoggerConsole()
        my_logger.log_memory_start("get_nb_open_positions_from_state")

        # action
        nb = grid.get_nb_open_positions_from_state("XRP")

        my_logger.log_memory_stop("get_nb_open_positions_from_state")

        # expectations
        assert(nb == 0)
