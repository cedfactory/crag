from abc import ABCMeta, abstractmethod
import pandas as pd

# for LoggerFile
import pathlib
import os.path

# for LoggerDiscordBot
import requests
import os
import discord
import xml.etree.cElementTree as ET

class ILogger(metaclass=ABCMeta):
    def __init__(self, params=None):
        pass

    @abstractmethod
    def log(self, msg, header="", author="", attachments=[]):
        pass

class LoggerConsole(ILogger):
    def __init__(self, params=None):
        pass

    def log(self, msg, header="", author="", attachments=[]):
        content = ""
        if author != "":
            content = content + "[{}] ".format(author)
        if header != "":
            content = content + "[{}] ".format(header)
        content = content + msg
        print(content)

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

    def log(self, msg, header="", author="", attachments=[]):
        if self.filename != "":
            with open(self.filename, 'a') as f:
                content = ""
                if author != "":
                    content = content + "[{}] ".format(author)
                if header != "":
                    content = content + "[{}] ".format(header)
                content = content + msg + "\n"
                f.write(content)

class LoggerDiscordBot(ILogger):
    def __init__(self, params=None):
        self.channel_id = None
        self.token = None
        self.webhook = None
        if params:
            self.channel_id = params.get("channel_id", self.channel_id)
            self.token = params.get("token", self.token)
            self.webhook = params.get("webhook", self.webhook)

    @staticmethod
    def _prepare_data_to_post(msg, header, author):
        #for all params, see https://discordapp.com/developers/docs/resources/webhook#execute-webhook
        data = {
            "content" : "",
            "username" : author
        }

        #for all params, see https://discordapp.com/developers/docs/resources/channel#embed-object
        if isinstance(msg, pd.DataFrame):
            msg = msg.to_string(index=False)
            msg = '```' + msg + '```'
        if isinstance(msg, list):
            data["embeds"] = [
                {
                    "title" : header,
                    "color" : int('0xff5733', base=16),
                    "fields": []
                }
            ]
            for str in msg:
                new_msg = {
                    "name" : str,
                    "value" : "",
                    "inline" : True
                }
                data["embeds"][0]["fields"].append(new_msg)
        else:
            data["embeds"] = [
                {
                    "description" : msg,
                    "title" : header,
                    "color" : int('0xff5733', base=16)
                }
            ]

        return data

    def log_webhook(self, msg, header, author, attachments=[]):
        # https://gist.github.com/Bilka2/5dd2ca2b6e9f3573e0c2defe5d3031b2
        data = self._prepare_data_to_post(msg, header, author)

        files = None
        # todo : manage several attachments
        if len(attachments) >= 1:
            files = {
                'file': (attachments[0], open(attachments[0], 'rb')),
            }
        result = requests.post(self.webhook, json = data, files = files)
        try:
            result.raise_for_status()
        except requests.exceptions.HTTPError as err:
            print(err)
        else:
            print("Payload delivered successfully, code {}.".format(result.status_code))

    def log_post(self, msg, header="", author="", attachments=[]):
        # https://stackoverflow.com/questions/69160500/discord-py-send-messages-outside-of-events
        if self.token is None or self.channel_id is None:
            return

        BASE_URL = f"https://discord.com/api/v9"
        SEND_URL = BASE_URL + "/channels/{id}/messages"

        headers = {
            "Authorization": f"Bot {self.token}",
            "User-Agent": f"DiscordBot"
        }

        content = ""
        if author != "":
            content = content + "[{}] ".format(author)
        if header != "":
            content = content + "[{}] ".format(header)
        content = content + msg
        r = requests.post(SEND_URL.format(id=self.channel_id), headers=headers, json={"content": content})

    def log(self, msg, header="", author="", attachments=[]):
        if self.webhook != "":
            self.log_webhook(msg, header, author, attachments)
        else:
            self.log_post(msg, header, author, attachments)
