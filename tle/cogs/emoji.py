import aiohttp
from discord import HTTPException
from discord.ext import commands

class EmojiCog(commands.Cog):
    session: aiohttp.ClientSession

    def __init__(self):
        self.session = aiohttp.ClientSession()

    @commands.command()
    @commands.is_owner()
    async def copy(self, ctx: commands.Context, name: str):
        avighna = ctx.bot.get_guild(1190034382560436274)
        for e in avighna.emojis:
            if e.name == name:
                await ctx.guild.create_custom_emoji(name=e.name, image=await e.read())
                await ctx.message.add_reaction("✅")
                break
        else:
            await ctx.reply("Emoji not found")

    @commands.command()
    @commands.is_owner()
    async def add(self, ctx: commands.Context, name: str, id: int):
        resp = await self.session.get(f"https://cdn.discordapp.com/emojis/{id}.png?size=240&quality=lossless")
        if not resp.ok:
            return await ctx.reply("Emoji not found")
        try:
            await ctx.guild.create_custom_emoji(name=name, image=await resp.read())
        except HTTPException:
            return await ctx.reply("Maximum emoji limit reached.")
        await ctx.message.add_reaction("✅")

async def setup(bot: commands.Bot):
    await bot.add_cog(EmojiCog())
