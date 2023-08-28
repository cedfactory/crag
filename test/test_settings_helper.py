import pytest
from src.toolbox import settings_helper

class TestSettingsHelper:

    def test_get_monitor_info(self):
        # action
        info = settings_helper.get_monitor_info("monitor_id", "./test/data/")

        # expectations
        assert(info.get("id") == "monitor_id")
        assert(info.get("url_base") == "theurlbase")
        assert(info.get("user") == "foobar")

    def test_get_mailbot_info(self):
        # action
        info = settings_helper.get_mailbot_info("default", "./test/data/")

        # expectations
        assert(info.get("id") == "default")
        assert(info.get("smtpserver") == "smtpserver.foobar.com")
        assert(info.get("port") == "666")
        assert(info.get("sender") == "sender@foobar.com")
        assert(info.get("password") == "password")

    def test_get_discord_bot_info(self):
        # action
        info = settings_helper.get_discord_bot_info("bot1", "./test/data/")

        # expectations
        assert (info.get("id") == "bot1")
        assert (info.get("token") == "tokenId")
        assert (info.get("channel") == "channelId")
        assert (info.get("webhook") == "webhook1")

    def test_get_fdp_url_info(self):
        # action
        info = settings_helper.get_fdp_url_info("fdp1", "./test/data/")

        # expectations
        assert (info.get("id") == "fdp1")
        assert (info.get("url") == "http://fdpurl.com")
