import pytest
import os
from src import broker_bitget_api


class TestBrokerBitget:

    def test_initialize_no_cash(self):
        return
        # action
        params = {"exchange": "bitget", "account": "account", "reset_account": False}
        my_broker = broker_bitget_api.BrokerBitGetApi(params)

        # expectations
        assert (my_broker._get_coin("XRPUSDT_UMCBL") == "XRP")
