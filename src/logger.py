from abc import ABCMeta, abstractmethod
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime
import time
import tracemalloc
from . import utils
from .toolbox import settings_helper
import json

# for LoggerConsole
from rich import print

# for LoggerFile
import pathlib
import os.path

# for LoggerDiscordBot
import requests
import os
import discord
import xml.etree.cElementTree as ET


def _initialize_crag_telegram_bot(botId=""):
    if botId == None or botId == "":
        return None

    if botId != None and botId != "":
        bot_info = settings_helper.get_telegram_bot_info(botId)
        token = bot_info.get("token", None)
        chat_id = bot_info.get("chat_id", None)
        return LoggerTelegramBot(params={"id":botId, "token":token, "chat_id":chat_id})

def _initialize_crag_discord_bot(botId=""):
    if botId == None or botId == "":
        return None

    if botId != None and botId != "":
        bot_info = settings_helper.get_discord_bot_info(botId)
        token = bot_info.get("token", None)
        channel_id = bot_info.get("channel", None)
        webhook = bot_info.get("webhook", None)
        return LoggerDiscordBot(params={"token":token, "channel_id":channel_id, "webhook":webhook})

    load_dotenv()
    token = os.getenv("CRAG_DISCORD_BOT_TOKEN")
    channel_id = os.getenv("CRAG_DISCORD_BOT_CHANNEL")
    webhook = os.getenv("CRAG_DISCORD_BOT_WEBHOOK")
    return LoggerDiscordBot(params={"token":token, "channel_id":channel_id, "webhook":webhook})

def get_loggers(str_loggers):
    lst_loggers = str_loggers.split(';')
    loggers = []
    for iter_logger in lst_loggers:
        logger_params = iter_logger.split("=")
        if logger_params[0] == "console":
            loggers.append(LoggerConsole())
        elif logger_params[0] == "file" and len(logger_params) == 2:
            loggers.append(LoggerFile({"filename": logger_params[1]}))
        elif logger_params[0] == "discordBot" and len(logger_params) == 2:
            loggers.append(_initialize_crag_discord_bot(logger_params[1]))
        elif logger_params[0] == "telegramBot" and len(logger_params) == 2:
            loggers.append(_initialize_crag_telegram_bot(logger_params[1]))
    return loggers

class ILogger(metaclass=ABCMeta):
    def __init__(self, params=None):
        self.timers = {}

    @abstractmethod
    def log(self, msg, header="", author="", attachments=None):
        pass

    def log_debug(self, msg, header="", author="", attachments=None):
        pass

    def log_info(self, msg, header="", author="", attachments=None):
        pass

    def log_warning(self, msg, header="", author="", attachments=None):
        pass

    def log_error(self, msg, header="", author="", attachments=None):
        pass

    def log_fatal(self, msg, header="", author="", attachments=None):
        pass

    def log_time_start(self, tag):
        start = time.time()
        self.timers[tag] = start
        del start
        del tag

    def log_time_stop(self, tag, msg=""):
        start = self.timers.get(tag, 0)
        if start == 0:
            self.log("!!! Tag {} unknown".format(tag), header="Timer")
        else:
            end = time.time()
            elapsed_time = str(utils.KeepNDecimals(end - start, 3))
            output = "{} : {} s ({})".format(tag, elapsed_time, msg)
            self.log(output, header="Timer")
            del end
            del elapsed_time
            del output
        del start

    def log_memory_usage(self, tag="", header="", author=""):
        rss = utils.get_memory_usage() / (1024 * 1024)
        self.log("memory usage @{} : {}".format(tag, rss), header=header, author=author)

    def log_memory_start(self, tag="", header="", author=""):
        tracemalloc.start()

    def log_memory_stop(self, tag="", header="", author=""):
        snapshot = tracemalloc.take_snapshot()

        # Stop tracing memory allocations
        tracemalloc.stop()

        current_directory = os.getcwd()

        parsed_stats = []

        # Iterate over the statistics in the snapshot
        for stat in snapshot.statistics('traceback'):
            # Check if any frame in the traceback is from your code
            #for frame in stat.traceback:
            #    if frame.filename.startswith(current_directory) and not frame.filename.startswith(os.path.join(current_directory,"venv")):
            #        print(frame.filename)
            #if any('crag_sim_3' in frame.filename for frame in stat.traceback):
            if any(frame.filename.startswith(current_directory) for frame in stat.traceback) and any(not frame.filename.startswith(os.path.join(current_directory, ".venv")) for frame in stat.traceback):
                traceback_lines = stat.traceback.format()  # Get the traceback lines
                # Parse the traceback lines
                file_location, line_number = traceback_lines[0].split(', line ')
                file_location = file_location.split('"')[1]
                line_number = int(line_number.strip())
                expression = traceback_lines[1].strip()
                size = stat.size
                count = stat.count
                # Append the parsed information to the list
                parsed_stats.append({
                    # "Traceback": traceback_lines,
                    "file location": file_location.replace(current_directory, ""),
                    "line": line_number,
                    "expression": expression,
                    "size": size,
                    "count": count
                })

                # Create a DataFrame from the parsed statistics
                df = pd.DataFrame(parsed_stats)

                df["iter"] = tag
                df["size"] = round(df["size"] / (1024 * 1024), 6)

                # Get the column you want to move
                column_to_move = df.pop('iter')

                # Insert the column at the first position
                df.insert(0, 'iter', column_to_move)
                print("total size: ", df["size"].sum())

                if True or (df["size"].sum() > 0.2):
                    print(df.to_string(index=False))
                    df.to_csv("df_"+str(tag)+".csv", index=False)
        del snapshot
        del df
        del column_to_move
        del current_directory
        return True

class LoggerConsole(ILogger):
    def __init__(self, params=None):
        super().__init__(params)

    def log(self, msg, header="", author="", attachments=[]):
        content = ""
        now = datetime.now()
        current_time = now.strftime("%Y/%m/%d %H:%M:%S.%f")
        content = content + "[{}] ".format(current_time)
        if author != "":
            content = content + "[{}] ".format(author)
        if header != "":
            content = content + "[{}] ".format(header)
        if isinstance(msg, dict):
            msg = str(msg)
        content = content + msg
        print(content)

class LoggerFile(ILogger):
    def __init__(self, params=None):
        super().__init__(params)
        self.filename_base = 'log'
        self.current_id = 0
        if params:
            self.filename_base = params.get("filename", self.filename_base)
        self.filename = self._get_current_filename()
        self.max_size = 10000000 # 10Mo

        if os.path.isdir(self.filename):
            self.filename = ""
        elif os.path.isfile(self.filename):
            open(self.filename, 'w').close()
        else:
            pathlib.Path(self.filename).touch()

    def _get_current_filename(self):
        return "{}{:04d}.log".format(self.filename_base, self.current_id)

    def _get_current_filesize(self):
        if self.filename != "" and os.path.isfile(self.filename):
            return os.stat(self.filename).st_size
        return 0

    def log(self, msg, header="", author="", attachments=[]):
        if self.filename != "":
            if self._get_current_filesize() > self.max_size:
                self.current_id += 1
                self.filename = self._get_current_filename()
            with open(self.filename, "a", encoding="utf-8") as f:
                content = ""
                now = datetime.now()
                current_time = now.strftime("%Y/%m/%d %H:%M:%S.%f")
                content = content + "[{}] ".format(current_time)
                if author != "":
                    content = content + "[{}] ".format(author)
                if header != "":
                    content = content + "[{}] ".format(header)
                content = content + msg + "\n"
                f.write(content)

class LoggerDiscordBot(ILogger):
    def __init__(self, params=None):
        super().__init__(params)
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
        # result = requests.post(self.webhook, json = data, files = files)
        with requests.post(self.webhook, json = data, files = files) as result:
            pass
        result.close()
        try:
            result.raise_for_status()
        except requests.exceptions.HTTPError as err:
            print(err)
        else:
            msg = "Payload delivered successfully, code {}.".format(result.status_code)
            #print(msg)
        del result
        del files
        del data
        locals().clear()

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
        r.close()
        del r
        locals().clear()

    def log(self, msg, header="", author="", attachments=[]):
        if self.webhook != "":
            self.log_webhook(msg, header, author, attachments)
        else:
            self.log_post(msg, header, author, attachments)


class LoggerTelegramBot(ILogger):
    def __init__(self, params=None):
        super().__init__(params)
        self.id = None
        self.token = None
        self.chat_id = None
        if params:
            self.id = params.get("id", self.id)
            self.token = params.get("token", self.token)
            self.chat_id = params.get("chat_id", self.chat_id)

    def log(self, msg, header="", author="", attachments=[], extra=None):
        response = None
        if self.id and self.token and self.chat_id:
            url = "https://api.telegram.org/bot" + self.token
            params = {"chat_id": self.chat_id, "text": msg, "parse_mode": "html"}
            if "message_id" in extra:
                if isinstance(extra["message_id"], int):
                    params["message_id"] = extra["message_id"]
                elif isinstance(extra["message_id"], list) and len(extra["message_id"]) > 1:
                    params["message_id"] = extra["message_id"][0]
                if attachments:
                    if len(attachments) == 1:
                        if os.path.exists(attachments[0]):
                            params["media"] = json.dumps({"type": "photo", "media": "attach://photo", "caption": msg, "parse_mode": "html", "show_caption_above_media": True})
                            imageFile = open(attachments[0], "rb")
                            try:
                                response = requests.post(url + "/editMessageMedia", files={"photo": imageFile}, data=params)
                            except (Exception) as e:
                                args = getattr(e, "args", [])
                                if len(args):
                                    print(args[0])
                    else:
                        # first delete the previous message and create a new one
                        if "message_id" in extra:
                            for message_id in extra["message_id"]:
                                requests.post(url + "/deleteMessage", data={"chat_id": self.chat_id, "message_id": message_id})

                        # create a new message
                        del extra["message_id"]
                        return self.log(msg, header=header, author=author, attachments=attachments, extra=extra)
                else:
                    response = requests.post(url + "/editMessageText", data=params)
            else:
                if attachments:
                    if len(attachments) == 1:
                        if os.path.exists(attachments[0]):
                            params["media"] = json.dumps({"type": "photo", "media": "attach://photo", "caption": msg, "parse_mode": "html", "show_caption_above_media": True})
                            imageFile = open(attachments[0], "rb")
                            response = requests.post(url + "/sendPhoto", files={"photo": imageFile}, data=params)
                    else:
                        media = []
                        files = {}
                        id = 0
                        for attachment in attachments:
                            if os.path.exists(attachment):
                                media.append({"type": "photo", "media": "attach://photo"+str(id), "caption": "attachment"})
                                imageFile = open(attachment, "rb")
                                files["photo"+str(id)] = imageFile
                                id = id + 1
                        params["media"] = json.dumps(media)
                        response = requests.post(url + "/sendMediaGroup", files=files, data=params)
                else:
                    response = requests.post(url + "/sendMessage", data=params)
        content = None
        if response:
            content = json.loads(response.content)
        return content

    def log_info(self, msg, header="", author="", attachments=None):
        self.log(msg, header, author, attachments)
