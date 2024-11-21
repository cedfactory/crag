import pytest
import os
import csv
from src import broker_simulation

class TestSimBroker:

    def test_initialize_no_cash(self):
        # action
        broker = broker_simulation.SimBroker()
        
        # expectations
        assert(broker.get_cash() == 10)

    def test_initialize_cash(self):
        # action
        broker = broker_simulation.SimBroker({"cash":100})

        # expectations
        assert(broker.get_cash() == 100)

    def test_get_commission(self):
        # context
        broker = broker_simulation.SimBroker()

        # action
        commission = broker.get_commission("FAKE")

        # expectations
        assert(commission == 0.0007)
