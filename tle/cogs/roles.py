import discord
import re
from datetime import datetime
from discord.ext import commands
from os import environ
from tle import constants
from tle.util import codeforces_common as cf_common, codeforces_api as cf_api

DISCUSSION_CHANNEL_ID = int(environ.get("MOCK_DISCUSSION_CHANNEL_ID", 0))
REMINDER_CHANNEL_ID = int(environ.get("MOCK_REMINDER_CHANNEL_ID", 0))
REMINDER_ROLE_ID = int(environ.get("MOCK_REMINDER_ROLE_ID", 0))

class RoleCogError(commands.CommandError):
    pass

async def get_solvers(contest_id: int) -> tuple[str, set[str]]:
    contest, _, ranklist = await cf_api.contest.standings(contest_id=contest_id, group_code="V9EnEktn91", as_manager=True, show_unofficial=True)
    time = datetime.now().timestamp() - contest.durationSeconds
    solvers = set()
    for row in ranklist:
        for member in row.party.members:
            if row.party.startTimeSeconds < time:
                solvers.add(member.handle)
    return contest.name, solvers

class JoinButton(
    discord.ui.DynamicItem[discord.ui.Button],
    template=r"button:join:(?P<contest_id>[0-9]+):(?P<thread_id>[0-9]+)"
):
    def __init__(self, contest_id: int, thread_id: int):
        super().__init__(
            discord.ui.Button(
                label="Join", style=discord.ButtonStyle.green,
                custom_id=f"button:join:{contest_id}:{thread_id}"
            )
        )
        self.contest_id = contest_id
        self.thread_id = thread_id

    @classmethod
    async def from_custom_id(cls, interaction, item, match: re.Match[str]):
        contest_id = int(match["contest_id"])
        thread_id = int(match["thread_id"])
        return cls(contest_id, thread_id)

    async def callback(self, interaction: discord.Interaction):
        handle = cf_common.user_db.get_handle(interaction.user.id, interaction.guild_id) # type: ignore
        if not handle:
            await interaction.response.send_message("You need to set your handle first.", ephemeral=True)
            return
        await interaction.response.defer(thinking=True, ephemeral=True)
        contest, solvers = await get_solvers(self.contest_id)
        if handle not in solvers:
            await interaction.followup.send(f"Please attempt {contest} before accessing the discussion thread.", ephemeral=True)
            return
        thread = interaction.guild.get_channel(DISCUSSION_CHANNEL_ID).get_thread(self.thread_id) # type: ignore
        if not thread:
            raise RoleCogError(f"Thread {self.thread_id} not found.")
        await thread.add_user(interaction.user)
        await interaction.followup.send("Successfully joined the discussion thread.", ephemeral=True)

class AccessView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

class RolesCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        bot.add_view(AccessView())
        bot.add_dynamic_items(JoinButton)

    async def cog_unload(self):
        self.bot.remove_dynamic_items(JoinButton)

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
        role = guild.get_role(role_id)
        if not role or role in member.roles: return
        if role.name == "off topic" and any(r.name == "8th or below" for r in member.roles):
            return
        await member.add_roles(role)

    @commands.Cog.listener()
    async def on_raw_reaction_remove(self, payload: discord.RawReactionActionEvent):
        role_id = cf_common.user_db.get_role_reaction(payload.message_id, str(payload.emoji)) # type: ignore
        if not role_id or not payload.guild_id: return
        guild = self.bot.get_guild(payload.guild_id)
        if not guild: return
        member = guild.get_member(payload.user_id)
        if not member: return
        await member.remove_roles(discord.Object(id=role_id))

    # Mock Discussion Role
    @commands.group(invoke_without_command=True)
    async def discussion(self, ctx: commands.Context):
        await ctx.send_help(ctx.command)

    async def setup_discussion(
        self, ctx: commands.Context, *, title: str | None = None,
        solvers: set[str] | None = None, contest_id: int, thread_id: int
    ):
        if not (title and solvers):
            title, solvers = await get_solvers(contest_id)

        discussions: discord.TextChannel = ctx.guild.get_channel(DISCUSSION_CHANNEL_ID) # type: ignore
        color = None
        if title.lower().startswith("zco"):
            color = 0x2ecc71
        elif title.lower().startswith("inoi"):
            color = 0x3498db
        elif title.lower().startswith("ioitc"):
            color = 0x9b59b6
        view = AccessView()
        view.add_item(JoinButton(contest_id, thread_id))
        await discussions.send(
            embed=discord.Embed(
                title=f"{title} Discussion", color=color,
                description="Please click the button below to join the discussion thread."
            ),
            view=view
        )

    @discussion.command() # type: ignore
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def create(self, ctx: commands.Context, message: discord.Message, contest_id: int):
        title, solvers = await get_solvers(contest_id)

        reminders: discord.TextChannel = ctx.guild.get_channel(REMINDER_CHANNEL_ID) # type: ignore
        await reminders.send(f"<@&{REMINDER_ROLE_ID}> The contest window for [{title}]({message.jump_url}) has officially begun. To start your contest window, click on the 'virtual participation' button right below the contest's name on [Codeforces](https://codeforces.com/group/pdEaEqYLGP/contests). As a reminder, we *recommend* that you participate tomorrow from 2:00 PM to 5:00 PM, but it is up to you.\n\nGood luck!")

        discussions: discord.TextChannel = ctx.guild.get_channel(DISCUSSION_CHANNEL_ID) # type: ignore
        thread = await discussions.create_thread(name=f"{title} Discussion", type=discord.ChannelType.private_thread, invitable=False)
        await thread.send(f"Please use this thread to discuss [{title}]({message.jump_url}).")

        await self.setup_discussion(
            ctx, title=title, solvers=solvers, contest_id=contest_id, thread_id=thread.id
        )

    @discussion.command() # type: ignore
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def assign(self, ctx: commands.Context, contest_id: int, thread: discord.Thread):
        await self.setup_discussion(ctx, contest_id=contest_id, thread_id=thread.id)

async def setup(bot: commands.Bot):
    await bot.add_cog(RolesCog(bot))
