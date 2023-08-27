import pytest
from src.toolbox import settings_helper

class TestSettingsHelper:

    def test_get_monitor_info(self):
        # action
        monitor = settings_helper.get_monitor_info("monitor_id", "./test/data/")

        # expectations
        assert(monitor.get("id") == "monitor_id")
        assert(monitor.get("url_base") == "theurlbase")
        assert(monitor.get("user") == "foobar")

    def test_get_mailbot_info(self):
        # action
        mailbot = settings_helper.get_mailbot_info("default", "./test/data/")

        # expectations
        assert(mailbot.get("id") == "default")
        assert(mailbot.get("smtpserver") == "smtpserver.foobar.com")
        assert(mailbot.get("port") == "666")
        assert(mailbot.get("sender") == "sender@foobar.com")
        assert(mailbot.get("password") == "password")
