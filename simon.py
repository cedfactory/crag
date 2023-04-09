import discord
from discord.ext import commands
import os
from dotenv import load_dotenv
import pika
import pandas as pd
from src import broker_ccxt,broker_bitget_api

class BotSimon(commands.Bot):
    def __init__(self):
        super().__init__(command_prefix="/")
        
        @self.command(name="hello")
        async def custom_command(ctx):
            print("hello")
            await ctx.channel.send("Hello {}".format(ctx.author.name))
        
        @self.command(name="open_positions")
        async def custom_command(ctx, *args):
            account = ""
            if len(args) >= 1:
                account = args[0]

            # get open positions from the broker
            my_broker = broker_bitget_api.BrokerBitGetApi()
            open_positions = my_broker.get_open_position()
            open_positions.drop("available", axis=1, inplace=True)
            open_positions.reset_index(drop=True, inplace=True)

            # convert dataframe to string to display
            msg = open_positions.to_string(index=True)
            msg = '```' + msg + '```'
            #msg += "Total : {:2f}".format(value)

            embed=discord.Embed(title=account, description=msg, color=0xFF5733)
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
            account = ""
            if len(args) >= 1:
                account = args[0]

            # get open positions from the broker
            my_broker = broker_bitget_api.BrokerBitGetApi()
            original_positions = my_broker.execute_reset_account()
            log_positions = original_positions[["symbol", "holdSide", "leverage", "usdtEquity"]]

            # convert dataframe to string to display
            msg = log_positions.to_string(index=True)
            msg = '```' + msg + '```'

            embed=discord.Embed(title=account, description=msg, color=0xFF5733)
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
