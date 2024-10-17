import pytest
from src import logger
import os
import pandas as pd

class TestLoggerFile:

    def test_logger_file_constructor(self):
        # context
        filename = "./test/log"
        expected_filename = "./test/log0000.log"
        params = {"filename": filename}
        if os.path.isfile(expected_filename):
            os.remove(expected_filename)

        # action
        my_logger = logger.LoggerFile(params)

        # expectations
        assert(os.path.isfile(expected_filename))


    def test_logger_file_get_current_filename(self):
        # context
        filename = "./test/log"
        expected_filename = "./test/log0000.log"
        params = {"filename": filename}
        my_logger = logger.LoggerFile(params)

        # action
        current_filename = my_logger._get_current_filename()
    
        # expectations
        assert(current_filename == expected_filename)

        # cleaning
        os.remove(expected_filename)

    def test_logger_file_log(self):
        # context
        filename = "./test/log"
        expected_filename = "./test/log0000.log"
        if os.path.isfile(expected_filename):
            os.remove(expected_filename)
        params = {"filename": filename}
        my_logger = logger.LoggerFile(params)

        # action
        my_logger.log("hello")

        # expectations
        assert (os.path.isfile(expected_filename))
        current_filesize = my_logger._get_current_filesize()
        assert (current_filesize == 28)
        with open(expected_filename) as f:
            lines = f.readlines()
            assert (len(lines) == 1)
            assert (lines[0].endswith("hello\n"))

        # cleaning
        os.remove(expected_filename)


    def test_logger_file_log_new_file(self):
        # context
        filename = "./test/log"
        expected_filename0 = "./test/log0000.log"
        expected_filename1 = "./test/log0001.log"
        if os.path.isfile(expected_filename0):
            os.remove(expected_filename0)
        if os.path.isfile(expected_filename1):
            os.remove(expected_filename1)
        params = {"filename": filename}
        my_logger = logger.LoggerFile(params)

        my_logger.max_size = 20 # change the max size
        my_logger.log("abcdefghijklmnopqrstuvwxyz")

        # action
        my_logger.log("0123456789")

        # expectations
        assert (os.path.isfile(expected_filename0))
        assert (os.path.isfile(expected_filename1))
        current_filesize = my_logger._get_current_filesize()
        assert (current_filesize == 40)
        with open(expected_filename0) as f:
            lines = f.readlines()
            assert (len(lines) == 1)
            assert (lines[0].endswith("abcdefghijklmnopqrstuvwxyz\n"))
        with open(expected_filename1) as f:
            lines = f.readlines()
            assert (len(lines) == 1)
            assert (lines[0].endswith("0123456789\n"))

        # cleaning
        os.remove(expected_filename0)
        os.remove(expected_filename1)


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

