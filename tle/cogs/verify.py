from dataclasses import dataclass
import discord
from discord.ext import commands
from datetime import datetime
from enum import Enum
from functools import partial
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build # type: ignore
from os import environ
from tle.util import codeforces_api as cf
from tle.constants import TLE_MODERATOR

class VerificationStatus(Enum):
    INELIGIBLE = -1
    NOT_APPLIED = 0
    PARTICIPANT = 1
    POSSIBLE_ALUMNI = 2

@dataclass
class Application:
    timestamp: datetime
    name: str
    class_: str
    dsc: str
    cf: str
    cc: str
    past: str
    oi: str
    exp: str
    duplicate: bool = False

GID = int(environ.get("UFDS_GUILD_ID", "0"))
VRID = int(environ.get("VERIFIED_ROLE_ID", "0"))
ARID = int(environ.get("ALUMNI_ROLE_ID", "0"))
ZCOID = int(environ.get("ZCO_TRACK_ROLE_ID", "0"))
UCID = int(environ.get("UNVERIFIED_CHANNEL_ID", "0"))
DCID = int(environ.get("DECISION_CHANNEL_ID", "0"))
SPREADSHEET_ID = environ.get("SPREADSHEET_ID")
RANGE = environ.get("RANGE")

class ConfirmView(discord.ui.View):
    def __init__(self, app: Application, user_id: int, role_id: int, accept_action, reject_action):
        super().__init__(timeout=None)
        self.app = app
        self.accept_action = accept_action
        self.reject_action = reject_action
        self.role_id = role_id
        self.user_id = user_id

    @discord.ui.button(emoji='✔️', style=discord.ButtonStyle.green)
    async def accept(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.message: return # just for typing
        await interaction.response.defer(ephemeral=True, thinking=True)
        await self.accept_action(self.user_id, self.role_id)
        await interaction.followup.send("✅ Successfully Accepted!", ephemeral=True)
        embed = interaction.message.embeds[0].copy()
        embed.colour = discord.Color.green()
        embed.description = "✅ **Accepted**"
        embed.set_footer(text=f"Accepted by {interaction.user}", icon_url=interaction.user.display_avatar.url)
        await interaction.message.edit(embed=embed, view=None)
        self.stop()

    @discord.ui.button(emoji="✖️", style=discord.ButtonStyle.red)
    async def reject(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True, thinking=True)
        await self.reject_action(interaction.user, self.app, self.user_id, interaction.message)
        await interaction.followup.send("❌ Successfully Rejected!", ephemeral=True)
        self.stop()

async def app_embed(
        app: Application, type_: str,
        color: discord.Color | int | None = None, description: str | None = None
):
    embed = discord.Embed(
        title=f"New Application: {app.dsc}",
        timestamp=app.timestamp,
        description=description,
        color=color
    )
    if app.duplicate:
        embed.add_field(name="⚠️ Warning", value="This user has submitted multiple applications.", inline=False)
    embed.add_field(name="Type", value=type_)
    embed.add_field(name="Name", value=app.name)
    embed.add_field(name="Class", value=app.class_)
    try:
        users = await cf.user.info(handles=[app.cf])
    except cf.HandleNotFoundError:
        embed.add_field(name="Codeforces", value=f"{app.cf} [⚠️ User not found]")
    else:
        value = f"[{app.cf}](https://codeforces.com/profile/{app.cf})"
        if users[0].rating is None:
            value += " [Unrated]"
        else:
            value += f" [{users[0].rating} current, {users[0].maxRating} max]"
        embed.add_field(name="Codeforces", value=value)
    if app.cc.strip():
        embed.add_field(name="CodeChef", value=app.cc.strip())
    if app.past != "None of the above":
        embed.add_field(name="Past ICO Experience", value="- " + "\n- ".join(app.past.split(", ")))
    if app.oi.strip():
        embed.add_field(name="Affiliation With Other OIs", value=app.oi.strip(), inline=False)
    if app.exp.strip():
        embed.add_field(name="Past Competitive Programming Experience", value=app.exp.strip(), inline=False)
    return embed

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

    def check_verification_sync(self, username: str) -> tuple[VerificationStatus, Application | None]:
        result = self.sheet_query.execute()
        values = result.get("values", [])[::-1][:-1]
        if not values:
            raise ValueError("No data found")
        values = [row for row in values if row[4].strip() == username]

        for row in values:
            while len(row) < 10:
                row.append("")
            timestamp, name, class_, eligible, dsc, cf, cc, past, oi, exp = row
            status = VerificationStatus.PARTICIPANT
            if eligible == "No" or class_ == "College or above":
                if past == "None of the above": status = VerificationStatus.INELIGIBLE
                else: status = VerificationStatus.POSSIBLE_ALUMNI
            return status, Application(
                datetime.strptime(timestamp, "%m/%d/%Y %H:%M:%S"), name,
                class_, dsc, cf, cc, past, oi, exp, len(values) > 1
            )

        return VerificationStatus.NOT_APPLIED, None

    async def check_verification(self, member: discord.Member) -> tuple[VerificationStatus, Application | None]:
        status, app = await self.bot.loop.run_in_executor(None, partial(self.check_verification_sync, member.name))
        if status == VerificationStatus.PARTICIPANT:
            await member.add_roles(discord.Object(VRID), discord.Object(ZCOID))
        return status, app

    async def dm(self, member: discord.Member, *args, **kwargs):
        try:
            await member.send(*args, **kwargs)
        except:
            pass

    async def accept_user(self, user_id: int, role_id: int):
        member = self.bot.get_guild(GID).get_member(user_id) # type: ignore
        if not member: return
        await member.add_roles(discord.Object(VRID), discord.Object(role_id))
        await self.dm(member, embed=discord.Embed(
            title="🎉 Congratulations!",
            description="You have been accepted into the SPOI program! You now have access to the server and have been assigned to the ZCO track.",
            color=discord.Color.green()
        ))

    async def reject_user(
            self, executor: discord.Member, app: Application, user_id: int, message: discord.Message
    ):
        embed = message.embeds[0].copy()
        embed.colour = discord.Color.red()
        embed.description = "❌ **Rejected**"
        embed.set_footer(text=f"Rejected by {executor}", icon_url=executor.display_avatar.url)
        await message.edit(embed=embed, view=None)
        member = self.bot.get_guild(GID).get_member(user_id) # type: ignore
        if not member: return
        await self.dm(member, embed=discord.Embed(
            title="❌ Application Rejected",
            description="We regret to inform you that your application has been rejected. In case of any issues, please contact oviyangandhi (<@755668598525132810>) on discord.",
            color=discord.Color.red()
        ))
        await member.kick(reason="Application Rejected")

    async def participant_application(
            self, app: Application, user_id: int, message: discord.Message | None = None
    ):
        embed = await app_embed(app, "Participant", discord.Color.green())
        view = ConfirmView(app, user_id, ZCOID, self.accept_user, self.reject_user)
        if message:
            await message.edit(embed=embed, view=view)
        else:
            await self.bot.get_channel(DCID).send(embed=embed, view=view) # type: ignore

    async def reject_alumni(self, executor: discord.Member, app: Application, user_id: int, message: discord.Message):
        await self.participant_application(app, user_id, message)

    async def create_application(self, member: discord.Member, not_applied_callback):
        status, app = await self.check_verification(member)
        if status == VerificationStatus.NOT_APPLIED:
            await not_applied_callback()
        elif app and status == VerificationStatus.POSSIBLE_ALUMNI:
            embed = await app_embed(
                app, "Alumni", 0x00ffff,
                "Is this a UFDS alumnus? If not, then you can still accept them as a participant."
            )
            channel: discord.TextChannel = self.bot.get_channel(DCID) # type: ignore
            await channel.send(
                embed=embed, view=ConfirmView(app, member.id, ARID, self.accept_user, self.reject_alumni)
            )
        elif app and status == VerificationStatus.INELIGIBLE:
            await self.participant_application(app, member.id)

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        if member.guild.id != GID: return
        await self.create_application(
            member,
            lambda: self.dm(member, f"Please fill [this form](https://forms.gle/FJPfWg2cD9SJL4mD6) and use the `;verify` command in the <#{UCID}> channel to get verified!")
        )

    @commands.command()
    @commands.has_role(TLE_MODERATOR)
    @commands.check(lambda ctx: ctx.channel.id == DCID) # type: ignore
    async def create(self, ctx: commands.Context, member: discord.Member):
        await self.create_application(member, lambda: ctx.reply("This user has not applied yet."))

    @commands.command()
    @commands.cooldown(3, 5*60, commands.BucketType.user)
    @commands.check(lambda ctx: ctx.channel.id == UCID and not ctx.author.get_role(VRID)) # type: ignore
    async def verify(self, ctx: commands.Context):
        status, app = await self.check_verification(ctx.author) # type: ignore
        if status == VerificationStatus.NOT_APPLIED:
            await ctx.reply("Please fill [this form](https://forms.gle/FJPfWg2cD9SJL4mD6) and use this command again to get verified!")
        elif status == VerificationStatus.PARTICIPANT:
            await ctx.message.add_reaction("✅")
        else:
            await ctx.reply("Your application is currently under review, and a lead trainer will verify you once you have been accepted into the program.")

    @verify.error
    async def verify_error(self, ctx: commands.Context, exc: commands.CommandError):
        if isinstance(exc, commands.CheckFailure):
            if isinstance(ctx.author, discord.Member) and ctx.author.get_role(VRID):
                await ctx.reply("You are already verified!")
            exc.handled = True # type: ignore

async def setup(bot: commands.Bot):
    await bot.add_cog(VerifyCog(bot))
