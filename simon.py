import discord
from discord.ext import commands
import os
from dotenv import load_dotenv
import pika
import pandas as pd
from src import accounts,broker_bitget_api,strategy_monitoring

class BotSimon(commands.Bot):
    def __init__(self):
        super().__init__(command_prefix="/")
        
        @self.command(name="hello")
        async def custom_command(ctx):
            print("hello")
            await ctx.channel.send("Hello {}".format(ctx.author.name))

        @self.command(name="commands")
        async def custom_command(ctx):
            msg = "Available commands for accounts:\n"
            msg += "- accounts : list of the registered accounts\n"
            msg += "- open_positions <account_id> : open positions for a specified account\n"
            msg += "- reset <account_id> : reset a specified account\n"
            msg += "Available commands for strategies:\n"
            msg += "- strategies : list of the alive strategies\n"
            await ctx.channel.send(msg)

        @self.command(name="accounts")
        async def custom_command(ctx, *args):
            accounts_info = accounts.import_accounts()
            lst_accounts = []
            lst_usdt_equities = []
            for key, value in accounts_info.items():
                account_id = value.get("id", "")
                my_broker = None
                broker_name = value.get("broker", "")
                if broker_name == "bitget":
                    my_broker = broker_bitget_api.BrokerBitGetApi({"account": account_id, "reset_account": False})
                    if my_broker:
                        usdt_equity = my_broker.get_usdt_equity()

                        lst_accounts.append(account_id)
                        lst_usdt_equities.append(usdt_equity)

            df = pd.DataFrame({"Accounts": lst_accounts, "USDT_Equity": lst_usdt_equities},
                              index=range(len(lst_accounts)))
            msg = '```' + df.to_string(index=False) + '```'

            embed=discord.Embed(title="accounts", description=msg, color=0xFF5733)
            await ctx.channel.send(embed=embed)

        @self.command(name="open_positions")
        async def custom_command(ctx, *args):
            if len(args) < 1:
                embed = discord.Embed(title="open_positions", description="? which account ?", color=0xFF5733)
                await ctx.channel.send(embed=embed)
                return

            account_id = args[0]

            # get open positions from the broker
            my_broker = broker_bitget_api.BrokerBitGetApi({"account": account_id, "reset_account": False})
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

            embed=discord.Embed(title=account, description=msg, color=0xFF5733)
            await ctx.channel.send(embed=embed)

        @self.command(name="reset")
        async def custom_command(ctx, *args):
            if len(args) < 1:
                embed = discord.Embed(title="reset", description="? which account ?", color=0xFF5733)
                await ctx.channel.send(embed=embed)
                return

            account_id = args[0]
            broker_bitget_api.BrokerBitGetApi({"account": account_id, "reset_account": True})

            embed = discord.Embed(title="reset".format(account_id), description="reset done on {}".format(account_id),
                                  color=0xFF5733)
            await ctx.channel.send(embed=embed)

        @self.command(name="strategies")
        async def custom_command(ctx, *args):
            accounts_info = accounts.import_accounts()
            account_ids = accounts_info.keys()
            lst_accounts = []
            lst_strategies = []
            msg_error = ""
            for account_id in account_ids:
                strategy_id = ""
                # rpc to the server
                try:
                    client = strategy_monitoring.StrategyMonitoringClient()
                    strategy_id = client.GetStrategyOnAccount(account_id).decode()
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
                    #client = strategy_monitoring.StrategyMonitoringClient()
                    #client.StopStrategy(strategy_id)
                    strategy_monitoring.publish_strategy_stop(strategy_id)
                    msg = "[Simon] stop command sent to {}".format(strategy_id)
                except:
                    msg = "[Simon] Problem encountered while sending a notification (publish_strategy_stop)"
            else:
                msg = "{} unknown".format(command)

            embed=discord.Embed(title="strategy", description=msg, color=0xFF5733)
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
    token = os.getenv("SIMON_DISCORD_BOT_TOKEN")
    bot = BotSimon()
    bot.run(token)
  

# Press the green button in the gutter to run the script.
if __name__ == "__main__":
    launch_simon()
