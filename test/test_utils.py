import pytest
from src import utils

class TestUtils:

    def test_fdp_request_ko_bad_service(self, mocker):
        # context
        mocker.patch('os.getenv', side_effect=[None])

        # action
        response_json = utils.fdp_request({"service":"fake_service"})

        # expectations
        assert(response_json["status"] == "ko")
        assert(response_json["info"] == "fdp url not found")

    def test_fdp_request_ko_unknown_service(self, mocker):
        # context
        mocker.patch('os.getenv', side_effect=["fake_fdp_url/"])

        # action
        response_json = utils.fdp_request({"service":"fake_service"})

        # expectations
        assert(response_json["status"] == "ko")
        assert(response_json["info"] == "unknown service")

    def test_fdp_request_ko_bad_fdp_url_no_multithreading(self, mocker):
        # context
        mocker.patch('os.getenv', side_effect=["fake_fdp_url/"])

        # action
        response_json = utils.fdp_request({"service":"history", "exchange":"ftx", "symbol":"BTC_USD", "start":"2022-01-01", "end": "2022-02-01", "interval": "1h"}, False)

        # expectations
        assert(response_json["status"] == "ko")
        print(response_json["info"])
        assert(response_json["info"] == "exception when requesting GET fake_fdp_url/history?exchange=ftx&start=2022-01-01&interval=1h&end=2022-02-01&symbol=BTC_USD")

    def test_fdp_request_ko_bad_fdp_url_multithreading(self, mocker):
        # context
        mocker.patch('os.getenv', side_effect=["fake_fdp_url/"])

        # action
        response_json = utils.fdp_request({"service":"history", "exchange":"ftx", "symbol":"BTC_USD", "start":"2022-01-01", "end": "2022-02-01", "interval": "1h"}, True)

        # expectations
        assert(response_json["status"] == "ok")
        assert(response_json["result"]["BTC_USD"]["status"] == "ko")
        assert(response_json["result"]["BTC_USD"]["info"] == "exception when requesting GET fake_fdp_url/history?exchange=ftx&start=2022-01-01&interval=1h&end=2022-02-01&symbol=BTC_USD")

    def test_get_variation_zero(self):
        # action
        variation = utils.get_variation(0, 750)

        # expectations
        assert(variation == 0)

    def test_get_variation_positive(self):
        # action
        variation = utils.get_variation(500, 750)

        # expectations
        assert(variation == 50)

    def test_get_variation_negative(self):
        # action
        variation = utils.get_variation(800, 200)

        # expectations
        assert(variation == -75)

    def test_get_random_id(self):
        # action
        id = utils.get_random_id()

        # expectations
        assert(id >= 10000000)
        assert(id < 99999999)
