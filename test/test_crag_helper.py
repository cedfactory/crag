import pytest
from src import crag_helper
from rich import print,inspect

class TestCrag:

    def test_initialization_from_configuration_file(self):
        # action
        bot = crag_helper.initialization_from_configuration_file("./test/data/strategy_grid_trading.xml")

        # expectations
        assert(bot.broker != None)
        assert(bot.rtstr.get_name() == "StrategyGridTrading")
        assert(bot.interval == 20)
        