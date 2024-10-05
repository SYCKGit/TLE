import discord
from datetime import datetime
from discord.ext import commands
from tle import constants
from tle.util import codeforces_common as cf_common, codeforces_api as cf_api

DISCUSSION_CHANNEL_ID = 1292109866835640320

class RoleCogError(commands.CommandError):
    pass

async def get_solvers(contest_id: int) -> tuple[str, set[str]]:
    contest, _, ranklist = await cf_api.contest.standings(contest_id=contest_id, group_code="pdEaEqYLGP", as_manager=True, show_unofficial=True)
    time = datetime.now().timestamp() - contest.durationSeconds
    solvers = set()
    for row in ranklist:
        for member in row.party.members:
            if row.party.startTimeSeconds < time:
                solvers.add(member.handle)
    return contest.name, solvers

class AccessView(discord.ui.View):
    def __init__(self, contest_id: int, thread_id: int):
        super().__init__(timeout=None)
        self.contest_id = contest_id
        self.thread_id = thread_id

    @discord.ui.button(label="Join", style=discord.ButtonStyle.green)
    async def join(self, interaction: discord.Interaction, button: discord.ui.Button):
        handle = cf_common.user_db.get_handle(interaction.user.id, interaction.guild_id) # type: ignore
        print(interaction.user.id, interaction.guild_id, handle)
        if not handle:
            await interaction.response.send_message("You need to set your handle first.", ephemeral=True)
            return
        contest, solvers = await get_solvers(self.contest_id)
        if handle not in solvers:
            await interaction.response.send_message(f"Pleae attempt {contest} before accessing the discussion thread.", ephemeral=True)
            return
        thread = interaction.guild.get_channel(DISCUSSION_CHANNEL_ID).get_thread(self.thread_id) # type: ignore
        if not thread:
            raise RoleCogError(f"Thread {self.thread_id} not found.")
        await thread.add_user(interaction.user)
        await interaction.response.send_message("Successfully joined the discussion thread.", ephemeral=True)

class RolesCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.thread_id: int | None = None
        self.contest_id: int | None = None

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

    # Mock Discussion Role
    @commands.group(invoke_without_command=True)
    async def discussion(self, ctx: commands.Context):
        await ctx.send_help(ctx.command)

    async def setup_discussion(self, ctx: commands.Context, *, title: str | None = None, solvers: set[str] | None = None):
        if not (self.contest_id and self.thread_id):
            raise RoleCogError("Contest ID or thread ID not set.")

        if not (title and solvers):
            title, solvers = await get_solvers(self.contest_id)

        discussions: discord.TextChannel = ctx.guild.get_channel(DISCUSSION_CHANNEL_ID) # type: ignore
        color = None
        if title.lower().startswith("zco"):
            color = 0x2ecc71
        elif title.lower().startswith("inoi"):
            color = 0x3498db
        elif title.lower().startswith("ioitc"):
            color = 0x9b59b6
        await discussions.send(
            embed=discord.Embed(
                title=f"{title} Discussion", color=color,
                description="Please click the button below to join the discussion thread."
            ),
            view=AccessView(self.contest_id, self.thread_id)
        )

        thread: discord.Thread = discussions.get_thread(self.thread_id) # type: ignore
        for handle in solvers:
            id = cf_common.user_db.get_user_id(handle, ctx.guild.id) # type: ignore
            if id:
                member = ctx.guild.get_member(id) # type: ignore
                if member:
                    await thread.add_user(member)

    @discussion.command() # type: ignore
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def create(self, ctx: commands.Context, message: discord.Message, contest_id: int):
        title, solvers = await get_solvers(contest_id)

        # tbh no one else is running this bot, I'm not gonna use annoying environment variables
        reminders: discord.TextChannel = ctx.guild.get_channel(1281630596098949122) # type: ignore
        await reminders.send(f"<@&1280987530178859072> The contest window for [{title}]({message.jump_url}) has officially begun. To start your contest window, click on the 'virtual participation' button right below the contest's name on [Codeforces](https://codeforces.com/group/pdEaEqYLGP/contests). As a reminder, we *recommend* that you participate tomorrow from 2:00 PM to 5:00 PM, but it is up to you.\n\nGood luck!")

        discussions: discord.TextChannel = ctx.guild.get_channel(DISCUSSION_CHANNEL_ID) # type: ignore
        thread = await discussions.create_thread(name=f"{title} Discussion", type=discord.ChannelType.private_thread, invitable=False)
        await thread.send(f"Please use this thread to discuss [{title}]({message.jump_url}).")

        self.thread_id = thread.id
        self.contest_id = contest_id
        await self.setup_discussion(ctx, title=title, solvers=solvers)

    @discussion.command() # type: ignore
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def assign(self, ctx: commands.Context, contest_id: int, thread: discord.Thread):
        self.contest_id = contest_id
        self.thread_id = thread.id
        await self.setup_discussion(ctx)

async def setup(bot: commands.Bot):
    await bot.add_cog(RolesCog(bot))