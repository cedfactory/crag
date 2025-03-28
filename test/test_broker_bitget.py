import pytest
import os, sys
sys.path.append(os.path.abspath("src"))

from src import broker_bitget_api
from . import utils

class TestBrokerBitget:

    def test_get_min_order_amount(self):
        # action
        params = {"exchange": "bitget", "account": "account", "reset_account_start": False}
        my_broker = broker_bitget_api.BrokerBitGetApi(params)

        # expectations
        assert (my_broker.get_min_order_amount("XRP") == 0.)

    @pytest.mark.skipif(utils.detect_environment() != "local", reason="Need local execution")
    def test_get_current_state(self):
        # context
        params = {"exchange": "bitget", "account": "bitget_cl1", "reset_account_start": False, }
        my_broker = broker_bitget_api.BrokerBitGetApi(params)

        # action
        current_state = my_broker.get_current_state(["XRP"])

        # expectations
        assert ('open_positions' in current_state.keys())
        assert ('open_orders' in current_state.keys())
        assert ('prices' in current_state.keys())
