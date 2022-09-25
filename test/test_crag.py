import pytest
import pandas as pd
import os
import json
from src import rtstr_super_reversal,crag,broker_simulation,trade

class TestCrag:

    def test_step_no_current_data(self):
        # context
        strategy_super_reversal = rtstr_super_reversal.StrategySuperReversal()
        simu_broker = broker_simulation.SimBroker({"data_directory":"fake_directory", "cash":10000})
        params = {"broker":simu_broker, "rtstr":strategy_super_reversal}
        bot = crag.Crag(params)

        # action
        bot.run()

        # expectations

    def test_step_trade(self):
        # context
        trade.Trade.reset_id()
        simu_broker = broker_simulation.SimBroker({"data_directory":"./test/data_sim_real_time_data_provider", "cash":10000})
        strategy_super_reversal = rtstr_super_reversal.StrategySuperReversal()
        crag_params = {"broker":simu_broker, "rtstr":strategy_super_reversal, "interval":1}
        bot = crag.Crag(crag_params)

        # action
        bot.run()

        # expectations
        assert(len(bot.current_trades) == 1)
        expected_trade = [0, '2021-01-17 15:00:00', '', 'BUY', '', '', 'AAVE/USD', 189.53, 189.53, 2.63625811, 499.65, 0.35000000000002274, '', 500.0, '', 9500.0, 499.65, 9999.65, -0.003500000000003638]
        expected_trade.append(None) # TO RESTORE : trade shouldn not contains this last element (gridzone)
        assert(bot.current_trades[0].get_csv_row()[1:] == expected_trade[1:])
        #assert(bot.current_trades[1].get_csv_row()[1:] == trade2[1:])
        
'''
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
