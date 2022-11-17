import pytest
from src import broker_ccxt

class TestBrokerCCXT:
    def test_constructor(self):
        # action
        broker = broker_ccxt.BrokerCCXT()

        # expectations
        assert(broker.simulation == False)
        assert(len(broker.trades) == 0)

    def test_get_value(self):
        # context
        broker = broker_ccxt.BrokerCCXT()

        # action
        value = broker.get_value("BTC/USD")

        # expectations
        assert(value == None)

    def test_get_cash(self):
        # context
        broker = broker_ccxt.BrokerCCXT()

        # action
        value = broker.get_cash()

        # expectations
        assert(value == None)
