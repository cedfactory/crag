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

    def test_get_ftp_account_info(self):
        # action
        info = settings_helper.get_ftp_account_info("ftp1", "./test/data/")

        # expectations
        assert (info.get("id") == "ftp1")
        assert (info.get("url") == "ftp.server.com")
        assert (info.get("port") == "21")
        assert (info.get("user") == "user1")
        assert (info.get("password") == "password1")

    def test_get_account_info(self):
        # action
        info = settings_helper.get_account_info("account1", "./test/data/")

        # expectations
        assert (info.get("id") == "account1")
        assert (info.get("attrib") == "attrib1")

    def test_get_accounts_info(self):
        # action
        info = settings_helper.get_accounts_info("./test/data/")

        # expectations
        assert (info["account1"].get("id") == "account1")
        assert (info["account1"].get("attrib") == "attrib1")
        assert (info["account2"].get("id") == "account2")
        assert (info["account2"].get("attrib") == "attrib2")
