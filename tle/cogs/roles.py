import discord
from discord.ext import commands
from tle import constants
from tle.util import codeforces_common as cf_common

class RolesCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @commands.group(invoke_without_command=True)
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def reactions(self, ctx: commands.Context):
        await ctx.send_help(ctx.command)

    @reactions.command() # type: ignore
    async def add(self, ctx: commands.Context, message: discord.Message, emoji: str, role: discord.Role):
        await message.add_reaction(emoji)
        cf_common.user_db.add_role_reaction(message.id, role.id, emoji) # type: ignore
        await ctx.message.add_reaction("✅")

    @reactions.command() # type: ignore
    async def remove(self, ctx: commands.Context, message: discord.Message, emoji: str):
        cf_common.user_db.remove_role_reaction(message.id, emoji) # type: ignore
        await ctx.message.add_reaction("✅")

    @commands.Cog.listener()
    async def on_raw_reaction_add(self, payload: discord.RawReactionActionEvent):
        role_id = cf_common.user_db.get_role_reaction(payload.message_id, str(payload.emoji)) # type: ignore
        if not role_id or not payload.guild_id: return
        guild = self.bot.get_guild(payload.guild_id)
        if not guild: return
        member = guild.get_member(payload.user_id)
        if not member: return
        await member.add_roles(discord.Object(id=role_id))

    @commands.Cog.listener()
    async def on_raw_reaction_remove(self, payload: discord.RawReactionActionEvent):
        role_id = cf_common.user_db.get_role_reaction(payload.message_id, str(payload.emoji)) # type: ignore
        if not role_id or not payload.guild_id: return
        guild = self.bot.get_guild(payload.guild_id)
        if not guild: return
        member = guild.get_member(payload.user_id)
        if not member: return
        await member.remove_roles(discord.Object(id=role_id))

async def setup(bot: commands.Bot):
    await bot.add_cog(RolesCog(bot))