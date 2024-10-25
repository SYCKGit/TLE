import discord
from discord.ext import commands

class FakeMessage:
    def __init__(self):
        self.mentions = []

class FakeContext:
    guild: discord.Guild
    bot: commands.Bot

    def __init__(self, guild: discord.Guild, bot: commands.Bot):
        self.guild = guild
        self.bot = bot
        self.message = FakeMessage()

class MemberConverter(commands.MemberConverter):
    async def convert(self, ctx: commands.Context, arg: str):
        guilds: list[discord.Guild] = list(ctx.bot.guilds)
        guilds.remove(ctx.guild)
        guilds.insert(0, ctx.guild)
        for guild in guilds:
            try:
                return await super().convert(FakeContext(guild, ctx.bot), arg)
            except commands.MemberNotFound:
                continue
        raise commands.MemberNotFound(arg)

class FunCog(commands.Cog):
    WEBHOOK_NAME = "TLE say-as"
    AVATARS = {
        "pink": "https://discord.com/assets/1b3106e166c99cc64682.png",
        "yellow": "https://discord.com/assets/b0f31fd761f131079ce0.png",
        "blue": "https://discord.com/assets/ac6f8cf36394c66e7651.png",
        "red": "https://discord.com/assets/0048cbfdd0b3ef186d22.png",
        "grey": "https://discord.com/assets/02b73275048e30fd09ac.png",
        "gray": "https://discord.com/assets/02b73275048e30fd09ac.png",
        "green": "https://discord.com/assets/259560fbae59ef36798b.png"
    }

    def __init__(self):
        self.webhooks: dict[int, discord.Webhook] = {}

    async def get_webhook(self, channel: discord.TextChannel) -> discord.Webhook:
        if channel.id not in self.webhooks:
            whs = await channel.webhooks()
            for wh in whs:
                if wh.name == self.WEBHOOK_NAME:
                    self.webhooks[channel.id] = wh
                    break
            else:
                self.webhooks[channel.id] = await channel.create_webhook(name=self.WEBHOOK_NAME)
        return self.webhooks[channel.id]

    async def perform_sayas(self, message: discord.Message, content: str, name: str, avatar: str):
        await message.delete()
        wh = await self.get_webhook(message.channel)
        try:
            await wh.send(content, username=name, avatar_url=avatar, allowed_mentions=discord.AllowedMentions(everyone=False, users=True, roles=False, replied_user=True))
        except discord.NotFound:
            del self.webhooks[channel.id]
            wh = await self.get_webhook(message.channel)
            await wh.send(content, username=name, avatar_url=avatar, allowed_mentions=discord.AllowedMentions(everyone=False, users=True, roles=False, replied_user=True))

    @commands.command(aliases=["say-as", "sayas"])
    async def say(self, ctx: commands.Context, user: MemberConverter, *, content: str):
        if user.id == ctx.bot.user.id:
            await ctx.message.delete()
            await ctx.send(content)
        await self.perform_sayas(ctx.message, content, user.display_name, user.display_avatar.url);

    @commands.command(aliases=["nsay-as", "nsayas"])
    async def nsay(self, ctx: commands.Context, name: str, avatar: str, *, content: str):
        await self.perform_sayas(ctx.message, content, name, self.AVATARS.get(avatar.lower(), avatar))

async def setup(bot):
    await bot.add_cog(FunCog())
