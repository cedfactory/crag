import pytest
import os
from src import broker_bitget_api


class TestBrokerBitget:

    def test_get_min_order_amount(self):
        # action
        params = {"exchange": "bitget", "account": "account", "reset_account": False}
        my_broker = broker_bitget_api.BrokerBitGetApi(params)

        # expectations
        assert (my_broker.get_min_order_amount("XRP") == 0.)
