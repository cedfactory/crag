import pytest
from src import crag_helper, crag
import os
from . import utils

class TestCrag:


    def test_initialization_from_configuration_file_ko_no_root(self):
        # context : create the configuration file
        configuration_file = utils.write_file("./test/generated/crag.xml", '''<root>
            <strategy name="StrategyUnknown" />
            <broker name="binance">
                <params account="test_bot" />
            </broker>
            <crag interval="20" />
        </root>''')

        # action
        configuration = crag_helper.load_configuration_file(configuration_file, config_path="./")

        # expectations
        assert(not configuration)

        # cleaning
        os.remove(configuration_file)

    def test_initialization_from_configuration_file_ko_unknown_broker(self):
        # context : create the configuration file
        configuration_file = utils.write_file("./test/generated/crag.xml", '''<configuration>
            <strategy name="StrategySuperReversal" />
            <broker name="fake_broker">
                <params account="test_bot" />
            </broker>
            <crag interval="20" />
        </configuration>''')

        # action
        configuration = crag_helper.load_configuration_file(configuration_file, config_path="./")
        params = crag_helper.get_crag_params_from_configuration(configuration)

        # expectations
        assert(not params)

        # cleaning
        os.remove(configuration_file)

    # unknown strategy
    def test_initialization_from_configuration_file_ko_unknown_strategy(self):
        # context : create the configuration file
        configuration_file = utils.write_file("./test/generated/crag.xml", '''<configuration>
            <strategy name="StrategyUnknown" />
            <broker name="binance">
                <params account="test_bot" />
            </broker>
            <crag interval="20" />
        </configuration>''')

        # action
        configuration = crag_helper.load_configuration_file(configuration_file, config_path="./")
        params = crag_helper.get_crag_params_from_configuration(configuration)

        # expectations
        assert(not params)

        # cleaning
        os.remove(configuration_file)

    # parse loggers
    def test_parse_loggers(self):
        # context : create the configuration file
        configuration_file = utils.write_file("./test/generated/crag.xml", '''<configuration>
            <strategy name="StrategyDummyTest" />
            <broker name="simulator">
                <params account="test_bot" reset_account_start="False" />
            </broker>
            <crag interval="20" loggers="console;file=output.log;discordBot=botId"/>
        </configuration>''')

        # action
        configuration = crag_helper.load_configuration_file(configuration_file, config_path="./")
        params = crag_helper.get_crag_params_from_configuration(configuration)

        # expectations
        assert(params != None)
        assert(len(params["loggers"]) == 3)

        # cleaning
        os.remove(configuration_file)

    def test_initialization_from_configuration_file_ok(self):
        return  # TODO : reactivate the test

        # context : create the configuration file
        configuration_file = utils.write_file("./test/generated/crag.xml", '''<configuration>
            <strategy name="StrategyGridTradingMulti">
                <params symbols="BTC/USD" grid_df_params="../test/data/multigrid_df_params.csv"/>
            </strategy>
            <broker name="bitget">
                <params exchange="bitget" account="test_bot" leverage="3" reset_account_start="False"/>
            </broker>
            <crag interval="20" />
        </configuration>''')

        # action
        configuration = crag_helper.load_configuration_file(configuration_file, config_path="./")
        params = crag_helper.get_crag_params_from_configuration(configuration)
        bot = crag.Crag(params)

        # expectations
        assert(bot.broker)
        assert(bot.broker.name == "simulator")
        assert(bot.broker.leverage == 3)
        assert(bot.rtstr.get_name() == "StrategyGridTradingMulti")
        assert(bot.rtstr.share_size == 10)
        assert(bot.rtstr.global_TP == 1000)
        assert(bot.interval == 20)

        # cleaning
        os.remove(configuration_file)

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
                <params cash="100" start_date="2022-01-01" end_date="2022-02-01" intervals="1d" reset_account_start="False"/>
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

