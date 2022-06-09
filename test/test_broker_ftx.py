import pytest
from src import broker_ftx

class TestBrokerFTX:
    def test_constructor(self):
        # action
        broker = broker_ftx.BrokerFTX()

        # expectations
        assert(broker.simulation == False)
        assert(len(broker.trades) == 0)
        assert(broker.authentificated == False)

    def test_get_value(self):
        # context
        broker = broker_ftx.BrokerFTX()

        # action
        value = broker.get_value("BTC/USD")

        # expectations
        assert(value != None)

    def test_get_cash(self):
        # context
        broker = broker_ftx.BrokerFTX()

        # action
        value = broker.get_cash()

        # expectations
        assert(value == None)
