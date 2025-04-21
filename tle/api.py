from discord.ext import commands
from fastapi import FastAPI
from uvicorn import Config, Server, run
import json
from os import path, environ

GID = int(environ.get("SPOI_GUILD_ID", "0"))
VRID = int(environ.get("VERIFIED_ROLE_ID", "0"))

verification_api = FastAPI()
verification_api.bot = None # type: ignore

@verification_api.get("/api/verified")
def is_verified(id: int):
    mem = verification_api.bot.get_guild(GID).get_member(id) # type: ignore
    return {"verified": bool(mem and mem.get_role(VRID))}

async def run_verification_api(bot: commands.Bot):
    verification_api.bot = bot # type: ignore
    kwargs = {}
    if path.exists("uvicorn.json"):
        with open("uvicorn.json", "r") as f:
            kwargs = json.load(f)
    await Server(Config(verification_api, **kwargs)).serve()