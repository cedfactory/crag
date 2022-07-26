import pytest
from src import logger
import os

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

