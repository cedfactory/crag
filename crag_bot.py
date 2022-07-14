import discord
import os
from dotenv import load_dotenv

load_dotenv()
token = os.getenv("CRAG_DISCORD_BOT_TOKEN")
channel = os.getenv("CRAG_DISCORD_BOT_CHANNEL")
if token == None or token == "":
    print("Token for crag bot not found")
    exit(0)

client = discord.Client()

@client.event
async def on_ready():
    print('Crag bot logged in as {} ({})'.format(client.user.name, client.user.id))

client.run(token)

