import pytest
import pandas as pd
import os
import json
from src import rtdp_tv,rtstr_tv,crag,broker
'''
class TestCrag:

    def test_run(self):
        params = {'infile':'./test/data/history.csv'}
        rtdp = rtdp_tv.RTDPTradingView(params)

        rtstr = rtstr_tv.RTStrTradingView()

        broker_simulation = broker.BrokerSimulation()
        broker_simulation.initialize({"cash":100})
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