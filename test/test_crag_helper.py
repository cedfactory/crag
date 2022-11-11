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

    # unknown broker
    def test_initialization_from_configuration_file_ko_unknown_strategy(self):
        # context : create the configruation file
        filename = self._write_file('''<configuration>
            <strategy name="StrategyGridTrading" />
            <broker name="fake_broker">
                <params account="test_bot" simulation="1" />
            </broker>
            <crag interval="20" />
        </configuration>''')

        # action
        bot = crag_helper.initialization_from_configuration_file(filename)

        # expectations
        assert(bot == None)

        # cleaning
        os.remove(filename)

    # unknown strategy
    def test_initialization_from_configuration_file_ko_unknown_strategy(self):
        # context : create the configruation file
        filename = self._write_file('''<configuration>
            <strategy name="StrategyUnknown" />
            <broker name="ftx">
                <params account="test_bot" simulation="1" />
            </broker>
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
            <broker name="ftx">
                <params account="test_bot" simulation="1"/>
            </broker>
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
            <strategy name="StrategyGridTrading">
                <params share_size="12" global_tp="15000" grid_step="1." grid_threshold=".8" upper_grid="25000" />
            </strategy>
            <broker name="ftx">
                <params account="test_bot" leverage="3" simulation="1"/>
            </broker>
            <crag interval="20" />
        </configuration>''')

        # action
        bot = crag_helper.initialization_from_configuration_file(filename)

        # expectations
        assert(bot.broker != None)
        assert(bot.broker.leverage == 3)
        assert(bot.rtstr.get_name() == "StrategyGridTrading")
        assert(bot.rtstr.share_size == 12)
        assert(bot.rtstr.global_tp == 15000)
        assert(bot.rtstr.grid.grid_threshold == .8)
        assert(bot.rtstr.grid.UpperPriceLimit == 25000)
        assert(bot.interval == 20)

        # cleaning
        os.remove(filename)

    # unknown broker
    def test_initialization_from_configuration_file_ok_broker_simulation(self):
        # context : create the configruation file
        filename = self._write_file('''<configuration>
            <strategy name="StrategyGridTrading" />
            <broker name="simulator">
                <params cash="100" intervals="1d"/>
            </broker>
            <crag interval="20" />
        </configuration>''')

        # action
        bot = crag_helper.initialization_from_configuration_file(filename)

        # expectations
        assert(bot.broker != None)
        #assert(bot.broker.start_date == "")
        #assert(bot.broker.end_date == "")
        assert(bot.broker.intervals == "1d")
        assert(bot.broker.get_cash() == 100)
        assert(bot.rtstr.get_name() == "StrategyGridTrading")
        assert(bot.rtstr.share_size == 10)
        assert(bot.rtstr.global_tp == 10000)
        assert(bot.rtstr.grid.grid_threshold == .08)
        assert(bot.rtstr.grid.UpperPriceLimit == 20500)
        assert(bot.interval == 20)

        # cleaning
        os.remove(filename)

    #
    # Backup
    #
    def test_backup(self):
        # context : create the configruation file
        filename = self._write_file('''<configuration>
            <strategy name="StrategyGridTrading" />
            <broker name="ftx" account="test_bot" simulation="1" />
            <crag interval="20" />
        </configuration>''')
        bot = crag_helper.initialization_from_configuration_file(filename)
        bot.backup()

        # action
        backup_filename = bot.backup_filename
        bot = crag_helper.initialization_from_pickle(backup_filename)

        # expectations
        assert(bot.broker != None)
        assert(bot.rtstr.get_name() == "StrategyGridTrading")
        assert(bot.interval == 20)

        # cleaning
        os.remove(filename)
        os.remove(backup_filename)
