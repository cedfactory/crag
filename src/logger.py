from abc import ABCMeta, abstractmethod

# for LoggerFile
import pathlib
import os.path

# for LoggerDiscordBot
import requests
import os


class ILogger(metaclass=ABCMeta):
    def __init__(self, params=None):
        pass

    @abstractmethod
    def log(self, msg):
        pass

class LoggerConsole(ILogger):
    def __init__(self, params=None):
        pass

    def log(self, msg):
        print(msg)

class LoggerFile(ILogger):
    def __init__(self, params=None):
        self.filename = 'log.txt'
        if params:
            self.filename = params.get("filename", self.filename)
        
        if os.path.isdir(self.filename):
            self.filename = ""
        elif os.path.isfile(self.filename):
            open(self.filename, 'w').close()
        else:
            pathlib.Path(self.filename).touch()

    def log(self, msg):
        if self.filename != "":
            with open(self.filename, 'a') as f:
                f.write(msg)

class LoggerDiscordBot(ILogger):
    def __init__(self, params=None):
        self.channel_id = None
        self.token = None
        if params:
            self.channel_id = params.get("channel_id", self.channel_id)
            self.token = params.get("token", self.token)
        
    def log(self, msg):
        # https://stackoverflow.com/questions/69160500/discord-py-send-messages-outside-of-events
        if self.token is None or self.channel_id is None:
            return

        BASE_URL = f"https://discord.com/api/v9"
        SEND_URL = BASE_URL + "/channels/{id}/messages"

        headers = {
            "Authorization": f"Bot {self.token}",
            "User-Agent": f"DiscordBot"
        }

        r = requests.post(SEND_URL.format(id=self.channel_id), headers=headers, json={"content": msg})

