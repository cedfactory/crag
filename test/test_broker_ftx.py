import pytest
from src import broker_ftx

class TestBrokerFTX:
    def test_constructor(self):
        # action
        broker = broker_ftx.BrokerFTX()

        # expectations
        assert(broker.simulation == False)
        print(broker.trades)
        assert(len(broker.trades) == 0)

    def test_initialize_authentification_ok(self):
        # context
        broker = broker_ftx.BrokerFTX()

        # action
        authentificated = broker.initialize({})

        # expectations
        assert(authentificated == False)
