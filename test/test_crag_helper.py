import pytest
from src import crag_helper
from rich import print,inspect
import os

class TestCrag:

    def _write_file(self, string):
        filename = "./test/generated/crag.xml"
        with open(filename, 'w') as f:
            f.write(string)
        return filename

    # unknown strategy
    def test_initialization_from_configuration_file_ko_unknown_strategy(self):
        # context : create the configruation file
        filename = self._write_file('''<configuration>
            <strategy name="StrategyUnknown" />
            <broker name="ftx" account="test_bot" simulation="1" />
            <crag interval="20" />
        </configuration>''')

        # action
        bot = crag_helper.initialization_from_configuration_file(filename)

        # expectations
        assert(bot == None)

        # cleaning
        os.remove(filename)

    def test_initialization_from_configuration_file_ko_no_root(self):
        # context : create the configruation file
        filename = self._write_file('''<root>
            <strategy name="StrategyUnknown" />
            <broker name="ftx" account="test_bot" simulation="1" />
            <crag interval="20" />
        </root>''')

        # action
        bot = crag_helper.initialization_from_configuration_file(filename)

        # expectations
        assert(bot == None)

        # cleaning
        os.remove(filename)

    def test_initialization_from_configuration_file_ok(self):
        # context : create the configruation file
        filename = self._write_file('''<configuration>
            <strategy name="StrategyGridTrading" />
            <broker name="ftx" account="test_bot" simulation="1" />
            <crag interval="20" />
        </configuration>''')

        # action
        bot = crag_helper.initialization_from_configuration_file(filename)

        # expectations
        assert(bot.broker != None)
        assert(bot.rtstr.get_name() == "StrategyGridTrading")
        assert(bot.interval == 20)

        # cleaning
        os.remove(filename)
