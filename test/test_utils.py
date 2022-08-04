import pytest
from src import utils

class TestUtils:

    def test_fdp_request_ko_bad_fdp_url(self, mocker):
        # context
        mocker.patch('os.getenv', side_effect=[None])

        # action
        response_json = utils.fdp_request("fake_service")

        # expectations
        assert(response_json["status"] == "ko")
        assert(response_json["info"] == "fdp url not found")

    def test_fdp_request_ko_bad_fdp_url(self, mocker):
        # context
        mocker.patch('os.getenv', side_effect=["fake_fdp_address"])

        # action
        response_json = utils.fdp_request("fake_service")

        # expectations
        assert(response_json["status"] == "ko")
        assert(response_json["info"] == "exception when requesting fake_fdp_address/fake_service")
