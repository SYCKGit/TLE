import discord
import json
from discord.ext import commands
from functools import partial
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from os import environ

GID = int(environ.get("UFDS_GUILD_ID", "0"))
VRID = int(environ.get("VERIFIED_ROLE_ID", "0"))
ARID = int(environ.get("ALUMNI_ROLE_ID", "0"))
ZCOID = int(environ.get("ZCO_TRACK_ROLE_ID", "0"))
UCID = int(environ.get("UNVERIFIED_CHANNEL_ID", "0"))
SPREADSHEET_ID = environ.get("SPREADSHEET_ID")
RANGE = environ.get("RANGE")

class VerifyCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        creds = Credentials.from_service_account_file("credentials.json")
        self.sheet_query = (
            build("sheets", "v4", credentials=creds)
                .spreadsheets()
                .values()
                .get(spreadsheetId=SPREADSHEET_ID, range=RANGE)
        )

    def check_verification_sync(self, name: str) -> int:
        result = self.sheet_query.execute()
        values = result.get("values", [])

        if not values:
            raise ValueError("No data found")

        found = 0
        for row in values[1:]:
            class_, eligible, dsc, cf, cc, past, oi, exp = row
            if dsc.strip() != name: continue
            found = 1
            if eligible == "No" or class_ == "College or above":
                if past == "None of the above": return -1
                else: found = 2

        return found

    async def check_verification(self, member: discord.Member) -> int:
        status = await self.bot.loop.run_in_executor(None, partial(self.check_verification_sync, member.name))
        if status > 0:
            await member.add_roles(discord.Object(VRID))
        if status == 1:
            await member.add_roles(discord.Object(ZCOID))
        if status == 2:
            await member.add_roles(discord.Object(ARID))
        return status

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        if member.guild.id != GID: return
        status = await self.check_verification(member)
        #if status == -1:
        #    try:
        #        await member.send("Sorry! You are not eligible to join this server!\nIn case of any issues, please contact oviyangandhi (<@755668598525132810>) on discord.")
        #    except:
        #        pass
        #    await member.kick(reason="Ineligible to join the server")

    @commands.command()
    @commands.cooldown(3, 5*60, commands.BucketType.user)
    @commands.check(lambda ctx: ctx.channel.id == UCID and not ctx.author.get_role(VRID))
    async def verify(self, ctx: commands.Context):
        status = await self.check_verification(ctx.author)
        if status == 0:
            await ctx.reply("Please fill [this form](https://forms.gle/FJPfWg2cD9SJL4mD6) and use this command again to get verified!")
        elif status > 0:
            await ctx.message.add_reaction("✅")
        else:
            await ctx.reply("Your application is currently under review, and a lead trainer will verify you once you have been accepted into the program.")

    @verify.error
    async def verify_error(self, ctx: commands.Context, exc: commands.CommandError):
        if isinstance(exc, commands.CheckFailure):
            exc.handled = True

async def setup(bot: commands.Bot):
    await bot.add_cog(VerifyCog(bot))
