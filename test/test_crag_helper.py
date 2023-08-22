import pytest
from src import crag_helper
from rich import print,inspect
import os
from . import utils

class TestCrag:

    # unknown broker
    def test_initialization_from_configuration_file_ko_unknown_broker(self):
        # context : create the configuration file
        filename = utils.write_file("./test/generated/crag.xml", '''<configuration>
            <strategy name="StrategySuperReversal" />
            <broker name="fake_broker">
                <params account="test_bot" simulation="1" />
            </broker>
            <crag interval="20" />
        </configuration>''')

        # action
        bot = crag_helper.initialization_from_configuration_file("../"+filename)

        # expectations
        assert(bot == None)

        # cleaning
        os.remove(filename)

    # unknown strategy
    def test_initialization_from_configuration_file_ko_unknown_strategy(self):
        # context : create the configuration file
        filename = utils.write_file("./test/generated/crag.xml", '''<configuration>
            <strategy name="StrategyUnknown" />
            <broker name="binance">
                <params account="test_bot" simulation="1" />
            </broker>
            <crag interval="20" />
        </configuration>''')

        # action
        bot = crag_helper.initialization_from_configuration_file("../"+filename)

        # expectations
        assert(bot == None)

        # cleaning
        os.remove(filename)

    def test_initialization_from_configuration_file_ko_no_root(self):
        # context : create the configuration file
        filename = utils.write_file("./test/generated/crag.xml", '''<root>
            <strategy name="StrategyUnknown" />
            <broker name="binance">
                <params account="test_bot" simulation="1"/>
            </broker>
            <crag interval="20" />
        </root>''')

        # action
        bot = crag_helper.initialization_from_configuration_file("../"+filename)

        # expectations
        assert(bot == None)

        # cleaning
        os.remove(filename)

    def test_initialization_from_configuration_file_ok(self):
        return  # TODO : reactivate the test

        # context : create the configuration file
        filename = utils.write_file("./test/generated/crag.xml", '''<configuration>
            <strategy name="StrategyGridTradingMulti">
                <params symbols="BTC/USD" grid_df_params="../test/data/multigrid_df_params.csv"/>
            </strategy>
            <broker name="simulator">
                <params exchange="simulator" account="test_bot" leverage="3" simulation="1" reset_account="False"/>
            </broker>
            <crag interval="20" />
        </configuration>''')

        # action
        bot = crag_helper.initialization_from_configuration_file("../"+filename)

        # expectations
        assert(bot.broker != None)
        assert(bot.broker.name == "simulator")
        assert(bot.broker.leverage == 3)
        assert(bot.rtstr.get_name() == "StrategyGridTradingMulti")
        assert(bot.rtstr.share_size == 10)
        assert(bot.rtstr.global_TP == 1000)
        assert(bot.interval == 20)

        # cleaning
        os.remove(filename)

    # unknown broker
    def test_initialization_from_configuration_file_ok_broker_simulation(self, mocker):
        return  # TODO : reactivate the test

        # context : create the configuration file
        json_df = utils.get_json_for_get_df_range()
        mocker.patch('src.utils.fdp_request_post', side_effect=[json_df])

        filename = utils.write_file("./test/generated/crag.xml", '''<configuration>
            <strategy name="StrategyGridTradingMulti">
                <params symbols="BTC/USD" grid_df_params="../test/data/multigrid_df_params.csv"/>
            </strategy>
            <broker name="simulator">
                <params cash="100" start_date="2022-01-01" end_date="2022-02-01" intervals="1d" reset_account="False"/>
            </broker>
            <crag interval="20" />
        </configuration>''')

        # action
        bot = crag_helper.initialization_from_configuration_file("../"+filename)

        # expectations
        assert(bot.broker != None)
        assert(bot.broker.name == "simulator")
        assert(bot.broker.start_date == "2022-01-01")
        assert(bot.broker.end_date == "2022-02-01")
        assert(bot.broker.intervals == "1d")
        assert(bot.broker.get_cash() == 100)
        assert(bot.rtstr.get_name() == "StrategyGridTradingMulti")
        assert(bot.rtstr.share_size == 10)
        assert(bot.rtstr.global_TP == 1000)
        assert(bot.interval == 20)

        # cleaning
        os.remove(filename)

