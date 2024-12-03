import discord
from discord.ext import commands

def can_use_solved(ctx: commands.Context):
    return (
        isinstance(ctx.channel, discord.Thread)
        and ctx.channel.parent_id in [1254146483310035048, 1281034435172499561]
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