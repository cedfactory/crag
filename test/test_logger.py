import pytest
from src import logger
import os
import pandas as pd

class TestLoggerFile:

    def test_constructor(self):
        # context
        filename = "./test/log.txt"
        if os.path.isfile(filename):
            os.remove(filename)
        params = {"filename":filename}

        # action
        my_logger = logger.LoggerFile(params)

        # expectations
        assert(os.path.isfile(filename))

        # cleaning
        os.remove(filename)

    def test_log(self):
        # context
        filename = "./test/log.txt"
        if os.path.isfile(filename):
            os.remove(filename)
        params = {"filename":filename}
        my_logger = logger.LoggerFile(params)

        # action
        my_logger.log("hello")
    
        # expectations
        assert(os.path.isfile(filename))
        with open(filename) as f:
            lines = f.readlines()
            assert(len(lines) == 1)
            assert(lines[0] == "hello")

        # cleaning
        os.remove(filename)


class TestLoggerDiscordBot:

    def test_constructor(self):
        # context
        params = {"channel_id":"123456", "token":"987654", "webhook":"123"}

        # action
        my_logger = logger.LoggerDiscordBot(params)

        # expectations
        assert(my_logger.channel_id == "123456")
        assert(my_logger.token == "987654")
        assert(my_logger.webhook == "123")

    def test_prepare_data_to_post_text(self):
        # context
        msg = "hello"
        header = "head"
        author = "me"
        my_logger = logger.LoggerDiscordBot()

        # action
        data = my_logger._prepare_data_to_post(msg, header, author)

        # expectations
        assert(data == {"content" : "", "username" : "me", "embeds": [{"description": "hello", "title": "head", "color": 16734003}]})

    def test_prepare_data_to_post_list(self):
        # context
        msg = ["hello1", "hello2"]
        header = "head"
        author = "me"
        my_logger = logger.LoggerDiscordBot()

        # action
        data = my_logger._prepare_data_to_post(msg, header, author)

        # expectations
        assert(data == {'content': '', 'username': 'me', 'embeds': [{'title': 'head', 'color': 16734003, 'fields': [{'name': 'hello1', 'value': '', 'inline': True}, {'name': 'hello2', 'value': '', 'inline': True}]}]})


    def test_prepare_data_to_post_dataframe(self):
        # context
        d = {'col1': [1, 2], 'col2': [3, 4]}
        df = pd.DataFrame(data=d)
        msg = df
        header = "head"
        author = "me"
        my_logger = logger.LoggerDiscordBot()

        # action
        data = my_logger._prepare_data_to_post(msg, header, author)

        # expectations
        assert(data == {'content': '', 'username': 'me', 'embeds': [{'description': '``` col1  col2\n    1     3\n    2     4```', 'title': 'head', 'color': 16734003}]})

