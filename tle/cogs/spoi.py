import discord
from discord.ext import commands
from os import environ

phid = int(environ.get("PROBLEM_HELP_CHANNEL_ID", 0))
ghid = int(environ.get("GENERAL_HELP_CHANNEL_ID", 0))

def can_use_solved(ctx: commands.Context):
    return (
        isinstance(ctx.channel, discord.Thread)
        and ctx.channel.parent_id in [phid, ghid]
        and ctx.channel.owner_id == ctx.author.id
    )

class SPOICog(commands.Cog):
    @commands.command()
    @commands.check(can_use_solved)
    async def solved(self, ctx: commands.Context):
        assert isinstance(ctx.channel, discord.Thread)
        await ctx.message.add_reaction("✅")
        await ctx.channel.edit(name=f"[SOLVED] {ctx.channel.name}", archived=True)

    @solved.error
    async def solved_error(self, ctx: commands.Context, exc: commands.CommandError):
        if isinstance(exc, commands.CheckFailure):
            exc.handled = True # type: ignore

async def setup(bot: commands.Bot):
    await bot.add_cog(SPOICog())