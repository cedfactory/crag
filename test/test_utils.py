import pytest
from src import utils

class TestUtils:

    def test_fdp_request_ko_bad_fdp_url(self, mocker):
        # context
        mocker.patch('os.getenv', side_effect=[None])

        # action
        response_json = utils.fdp_request({"service":"fake_service"})

        # expectations
        assert(response_json["status"] == "ko")
        assert(response_json["info"] == "fdp url not found")

    def test_fdp_request_ko_unknown_service(self, mocker):
        # context
        mocker.patch('os.getenv', side_effect=["fake_fdp_url"])

        # action
        response_json = utils.fdp_request({"service":"fake_service"})

        # expectations
        assert(response_json["status"] == "ko")
        assert(response_json["info"] == "unknown service")

    def test_fdp_request_ko_bad_fdp_url(self, mocker):
        # context
        mocker.patch('os.getenv', side_effect=["fake_fdp_url"])

        # action
        response_json = utils.fdp_request({"service":"history", "exchange":"ftx", "symbol":"BTC_USD", "start":"2022-01-01", "end": "2022-02-01", "interval": "1h"})

        # expectations
        assert(response_json["status"] == "ko")
        assert(response_json["info"] == "exception when requesting fake_fdp_url/history?exchange=ftx&symbol=BTC_USD&start=2022-01-01&interval=1h&end=2022-02-01")
