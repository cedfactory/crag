import discord
from discord.ext import commands
import os
from dotenv import load_dotenv
import pandas as pd
from src import broker_ftx

class BotSimon(commands.Bot):
    def __init__(self, queue=None):
        super().__init__(command_prefix="/")
        self.queue = queue
        
        @self.command(name="hello")
        async def custom_command(ctx):
            print("hello")
            await ctx.channel.send("Hello {}".format(ctx.author.name))
        
        @self.command(name="balance")
        async def custom_command(ctx, *args):
            subaccount = "test_bot"
            if len(args) >= 1:
                subaccount = args[0]

            # get balance from the broker
            my_broker_ftx = broker_ftx.BrokerFTX({'account':'test_bot', 'simulation':1})
            balance = my_broker_ftx.get_balance()

            # convert data into dataframe
            value = 0
            data = {"symbol": [], "usdValue": []}
            for symbol in balance:
                data["symbol"].append(symbol)
                usdValue = balance[symbol]["usdValue"]
                data["usdValue"].append(usdValue)
                value += usdValue
            df_result = pd.DataFrame(data)
            df_result.set_index("symbol", inplace=True)

            # convert dataframe to string to display
            msg = df_result.to_string(index=True)
            msg = '```' + msg + '```'
            msg += "Total : {:2f}".format(value)

            embed=discord.Embed(title=subaccount, description=msg, color=0xFF5733)
            await ctx.channel.send(embed=embed)

        @self.command(name="stop_crag")
        async def custom_command(ctx, *args):
            if self.queue:
                self.queue.put("stop")
            embed=discord.Embed(title="stop crag", description="stop crag", color=0xFF5733)
            await ctx.channel.send(embed=embed)
        
    async def on_ready(self):
        print("bot is ready")

def launch_simon(queue=None):
    load_dotenv()
    token = os.getenv("SIMON_DISCORD_BOT_TOKEN")
    bot = BotSimon(queue)
    bot.run(token)
  

# Press the green button in the gutter to run the script.
if __name__ == "__main__":
    launch_simon()
