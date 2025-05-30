import pytest

from src import crag,broker_simulation
from . import utils
from src import crag_helper

class TestCrag:
    '''
    def test_step_no_current_data(self, mocker):
        # context
        json_df = utils.get_json_for_get_df_range()
        mocker.patch('src.utils.fdp_request_post', side_effect=[json_df])

        symbol = "BTC/USDT"
        params = {"symbols": symbol, "grid_df_params": "./test/data/multigrid_df_params.csv"}
        strategy_multigrid = rtstr_grid_trading_multi.StrategyGridTradingMulti(params)
        simu_broker = broker_simulation.SimBroker({"data_directory":"fake_directory", "cash":10000, "start_date": "2022-01-01", "end_date": "2002-01-05"})
        params = {"broker":simu_broker, "rtstr":strategy_multigrid}
        bot = crag.Crag(params)

        # action
        bot.run()

        # expectations

    def test_step_trade(self, mocker):
        # context
        trade.Trade.reset_id()
        
        json_df = utils.get_json_for_get_df_range()
        mocker.patch('src.utils.fdp_request_post', side_effect=[json_df])

        symbol = "BTC/USDT"
        params = {"symbols": symbol, "grid_df_params": "./test/data/multigrid_df_params.csv"}
        strategy_multigrid = rtstr_grid_trading_multi.StrategyGridTradingMulti(params)
        simu_broker = broker_simulation.SimBroker({"data_directory":"fake_directory", "cash":10000, "start_date": "2022-01-01", "end_date": "2002-01-05"})
        params = {"broker":simu_broker, "rtstr":strategy_multigrid}
        bot = crag.Crag(params)

        # action
        bot.run()

        # expectations
        assert(len(bot.current_trades) == 1)
        expected_trade = [0, '2021-01-17 15:00:00', '', 'BUY', '', '', 'AAVE/USD', 189.53, 189.53, 2.63625811, 499.65, 0.35000000000002274, '', 500.0, '', 9500.0, 499.65, 9999.65, -0.003500000000003638]
        expected_trade.append(-1) # TO RESTORE : trade shouldn not contains this last element (gridzone)
        assert(bot.current_trades[0].get_csv_row()[1:] == expected_trade[1:])
        #assert(bot.current_trades[1].get_csv_row()[1:] == trade2[1:])

    def test_run(self):
        params = {'infile':'./test/data/history.csv'}
        rtdp = rtdp_tv.RTDPTradingView(params)

        rtstr = rtstr_tv.RTStrTradingView()

        broker_simulation = broker.BrokerSimulation({"cash":100})
        params = {'rtdp':rtdp, 'broker':broker_simulation, 'rtstr':rtstr}
        bot = crag.Crag(params)
        bot.run()

        expected_lst_symbols_to_buy = {
            0: ['GOG/USD', 'STEP/USD', 'SNX/USD', 'AAVE/USD', '1INCH/USD', 'DFL/USD', 'TLM/USD', 'ALCX/USD', 'LINA/USD', 'RSR/USD', 'DYDX/USD', 'LOOKS/USD', 'COMP/USD', 'DENT/USD', 'CITY/USD'],
            1: ['GOG/USD', 'STEP/USD', 'SNX/USD', 'LOOKS/USD', 'ALCX/USD', '1INCH/USD', 'DENT/USD', 'TLM/USD', 'DFL/USD', 'AAVE/USD', 'COMP/USD', 'MKR/USD', 'LINA/USD'],
            2: ['GOG/USD', 'STEP/USD', 'DENT/USD', 'SNX/USD', 'UNI/USD', 'LOOKS/USD', 'STG/USD', 'ALCX/USD', '1INCH/USD', 'DFL/USD', 'AAVE/USD', 'MKR/USD', 'TLM/USD', 'COMP/USD']
        }
        for i in range(3):
            assert(bot.log[i]["lst_symbols_to_buy"] == expected_lst_symbols_to_buy[i])
    '''

    #
    # Backup
    #
    def test_backup(self):
        return # TODO : reactivate the test

        # context : create the configruation file
        filename = utils.write_file("./test/generated/crag.xml", '''<configuration>
            <strategy name="StrategyGridTradingMulti">
                <params symbols="BTC/USD" grid_df_params="../test/data/multigrid_df_params.csv"/>
            </strategy>
            <broker name="simulator">
                <params exchange="simulator" account="test_bot" reset_account_start="False" />
            </broker>
            <crag interval="20" />
        </configuration>''')

        configuration = crag_helper.load_configuration_file("../"+filename)
        params = crag_helper.get_crag_params_from_configuration(configuration)
        bot = crag.Crag(params)

        # action 1 : backup
        bot.backup()

        # action 2 : restore
        backup_filename = bot.backup_filename
        bot = crag_helper.initialization_from_pickle(backup_filename)

        # expectations
        assert (bot.broker != None)
        assert (bot.broker.name == "simulator")
        assert (bot.rtstr.get_name() == "StrategyGridTradingMulti")
        assert (bot.interval == 20)

        # cleaning
        os.remove(filename)
        os.remove(backup_filename)
