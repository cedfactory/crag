import discord
from discord.ext import commands
from datetime import datetime
from dotenv import load_dotenv
import pika
import pandas as pd
from src import broker_helper,broker_bitget_api,utils
from src.toolbox import monitoring_helper,settings_helper


async def Send(ctx, embed):
    try:
        await ctx.channel.send(embed=embed)
    except Exception as e:
        print("!!!!!!! EXCEPTION RAISED !!!!!!!")
        print(e)

class BotSimon(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        super().__init__(command_prefix="/", intents=intents)


        @self.command(name="hello")
        async def custom_command(ctx):
            print("hello")
            await ctx.channel.send("Hello {}".format(ctx.author.name))

        @self.command(name="commands")
        async def custom_command(ctx):
            msg = "Available commands for accounts:\n"
            msg += "- bot : specify the amount for the bot account (ex : /bot 121.22)\n"
            msg += "- accounts : list of the registered accounts\n"
            msg += "- open_positions <account_id> : open positions for a specified account\n"
            msg += "- reset <account_id> : reset a specified account\n"
            msg += "Available commands for strategies:\n"
            msg += "- strategies : list of the alive strategies\n"
            msg += "Available commands for transferts:\n"
            msg += "- transfert <2023/07/23 18:56> <account_src> <account_dst> <amount> <comment>\n"
            msg += "Available commands for comments:\n"
            msg += "- comment <comment>\n"
            msg += "- comment transfert id <comment>\n"
            await ctx.channel.send(msg)

        @self.command(name="accounts")
        async def custom_command(ctx, *args):
            df = broker_helper.get_usdt_equity_all_accounts()
            msg = '```' + df.to_string(index=False) + '```'
            msg += '```Total : $ ' + str(utils.KeepNDecimals(df["USDT_Equity"].sum())) + '```'

            embed = discord.Embed(title="accounts", description=msg, color=0xFF5733)
            await Send(ctx, embed)
            #await ctx.channel.send(embed=embed)

        @self.command(name="open_positions")
        async def custom_command(ctx, *args):
            if len(args) < 1:
                embed = discord.Embed(title="open_positions", description="? which account ?", color=0xFF5733)
                await ctx.channel.send(embed=embed)
                return

            account_id = args[0]

            # get open positions from the broker
            my_broker = broker_bitget_api.BrokerBitGetApi({"account": account_id, "reset_account_start": False})
            if not my_broker:
                embed = discord.Embed(title="open_positions", description="! can't connect on this account !", color=0xFF5733)
                await ctx.channel.send(embed=embed)
                return

            open_positions = my_broker.get_open_position()
            useless_columns = ["available", "marginCoin", "total", "marketPrice", "averageOpenPrice", "achievedProfits", "usdtEquity", "unrealizedPL", "liquidationPrice"]
            open_positions.drop(useless_columns, axis=1, inplace=True)
            open_positions.reset_index(drop=True, inplace=True)

            # convert dataframe to string to display
            msg = open_positions.to_string(index=True)
            msg = '```' + msg + '```'

            embed=discord.Embed(title=account_id, description=msg, color=0xFF5733)
            await ctx.channel.send(embed=embed)
            
        @self.command(name="cash")
        async def custom_command(ctx, *args):
            account = ""
            if len(args) >= 1:
                account = args[0]

            # getcash
            my_broker = broker_bitget_api.BrokerBitGetApi()
            cash = my_broker.get_cash()

            # convert dataframe to string to display
            msg = "Cash : {:2f}".format(cash)

            embed = discord.Embed(title=account, description=msg, color=0xFF5733)
            await ctx.channel.send(embed=embed)

        @self.command(name="reset")
        async def custom_command(ctx, *args):
            if len(args) < 1:
                embed = discord.Embed(title="reset", description="? which account ?", color=0xFF5733)
                await ctx.channel.send(embed=embed)
                return

            account_id = args[0]
            broker_bitget_api.BrokerBitGetApi({"account": account_id, "reset_account_start": True})

            embed = discord.Embed(title="reset".format(account_id), description="reset done on {}".format(account_id),
                                  color=0xFF5733)
            await ctx.channel.send(embed=embed)

        @self.command(name="strategies")
        async def custom_command(ctx, *args):
            accounts_info = settings_helper.get_accounts_info()
            account_ids = accounts_info.keys()
            lst_accounts = []
            lst_strategies = []
            msg_error = ""
            for account_id in account_ids:
                strategy_id = ""
                # rpc to the server
                try:
                    monitor = monitoring_helper.SQLMonitoring("ovh_mysql")
                    #strategy_id = monitor.GetStrategyOnAccount(account_id).decode()
                except:
                    print("[Simon] Problem encountered while sending a rpc request")

                if strategy_id != "":
                    lst_accounts.append(account_id)
                    lst_strategies.append(strategy_id)

            msg = ""
            if msg_error != "":
                msg = msg_error
            else:
                df = pd.DataFrame({"Accounts": lst_accounts, "Strategies": lst_strategies},
                                  index=range(len(lst_accounts)))
                msg = '```' + df.to_string(index=False) + '```'

            embed=discord.Embed(title="strategies", description=msg, color=0xFF5733)
            await ctx.channel.send(embed=embed)


        @self.command(name="strategy")
        async def custom_command(ctx, *args):
            if len(args) < 2:
                message = args[0]
                self.send_message_to_crag(message)
                embed=discord.Embed(title="command {}".format(message), description="missing parameters", color=0xFF5733)
                await ctx.channel.send(embed=embed)
                return
            command = args[0]
            strategy_id = args[1]
            msg = ""
            if command == "stop":
                try:
                    monitor = monitoring_helper.SQLMonitoring("ovh_mysql")
                    monitor.send_strategy_stop(strategy_id)
                    msg = "[Simon] stop command sent to {}".format(strategy_id)
                except:
                    msg = "[Simon] Problem encountered while sending a notification (send_strategy_stop)"
            else:
                msg = "{} unknown".format(command)

            embed=discord.Embed(title="strategy", description=msg, color=0xFF5733)
            await ctx.channel.send(embed=embed)

        @self.command(name="bot")
        async def custom_command(ctx, *args):
            if len(args) != 1:
                embed=discord.Embed(title="bot", description="missing argument", color=0xFF5733)
                await ctx.channel.send(embed=embed)
            else:
                try:
                    value = float(args[0])
                except ValueError:  # using the except block
                    message = "can't convert the argument into a float"
                else:
                    with open("bot.txt", "w") as f:
                        f.write(str(value))
                        message = "value {} has been stored".format(value)

                if message == "":
                    message = "an error occurred"
                embed = discord.Embed(title="bot", description=message, color=0xFF5733)
                await ctx.channel.send(embed=embed)

        @self.command(name="crag")
        async def custom_command(ctx, *args):
            if len(args) != 1:
                embed=discord.Embed(title="crag", description="missing argument", color=0xFF5733)
                await ctx.channel.send(embed=embed)
            else:
                message = args[0]
                self.send_message_to_crag(message)
                embed=discord.Embed(title="crag {}".format(message), description="crag {}".format(message), color=0xFF5733)
                await ctx.channel.send(embed=embed)

        @self.command(name="transfert")
        async def custom_command(ctx, *args):
            if len(args) < 5:
                embed=discord.Embed(title="transfert", description="missing argument. type /commands", color=0xFF5733)
                await ctx.channel.send(embed=embed)
            else:
                msg = ""
                try:
                    date_str = " ".join(args[0:2])
                    date_obj = datetime.strptime(date_str, '%Y/%m/%d %H:%M')
                    timestamp = date_obj.timestamp()
                    account_src = args[2]
                    account_dst = args[3]
                    amount = args[4]
                    client_id = 1
                    comment = ""
                    if len(args) >= 5:
                        comment = " ".join(args[5:])

                    msg = "[Simon] send transfert from {} to {} : $ {} ( {} )".format(account_src, account_dst, amount, timestamp)
                    print(msg)

                    if account_src == "ext":
                        account_src = ""
                    if account_dst == "ext":
                        account_dst = ""

                    monitor = monitoring_helper.SQLMonitoring("ovh_mysql")
                    response_json = monitor.send_transfert(timestamp, account_src, account_dst, amount, client_id)
                    if response_json["status"] == "ok":
                        msg += " {} => id = {}".format(response_json["status"], response_json["result"])

                except:
                    msg = "[Simon] Problem encountered while sending a transfert (send_transfert)"

                embed=discord.Embed(title="transfert", description="{}".format(msg), color=0xFF5733)
                await ctx.channel.send(embed=embed)

        @self.command(name="comment")
        async def custom_command(ctx, *args):
            if len(args) < 1:
                embed=discord.Embed(title="comment", description="missing argument. type /commands", color=0xFF5733)
                await ctx.channel.send(embed=embed)
            else:
                msg = ""
                try:
                    comment = " ".join(args[0:])
                    ct = datetime.now()
                    timestamp = ct.timestamp()

                    monitor = monitoring_helper.SQLMonitoring("ovh_mysql")
                    response_json = monitor.send_comment(timestamp, comment)

                    if response_json["status"] == "ok":
                        msg = "[Simon] send comment \"{}\"".format(comment)
                        msg += " {} => id = {}".format(response_json["status"], response_json["result"])

                except:
                    msg = "[Simon] Problem encountered while sending a comment (send_comment)"

                embed=discord.Embed(title="comment", description="{}".format(msg), color=0xFF5733)
                await ctx.channel.send(embed=embed)

    async def on_ready(self):
        print("Simon is ready")

    def send_message_to_crag(self, message):
        connection = pika.BlockingConnection(pika.ConnectionParameters('127.0.0.1'))
        channel = connection.channel()
        channel.queue_declare(queue='crag')
        channel.basic_publish(exchange='', routing_key='crag', body=message)
        connection.close()

def launch_simon():
    load_dotenv()
    info = settings_helper.get_discord_bot_info("simon")
    token = info.get("token", "")
    bot = BotSimon()
    bot.run(token)
  

# Press the green button in the gutter to run the script.
if __name__ == "__main__":
    launch_simon()
