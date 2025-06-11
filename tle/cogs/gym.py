import asyncio
import datetime
import sqlite3

import discord
from discord.ext import commands

from tle import constants
from tle.util import codeforces_common as cf_common
from tle.util import discord_common
from tle.util import paginator

import zoneinfo
import dateutil.parser


day_to_int = {
    "mon": 0,
    "m": 0,
    "monday": 0,
    "tue": 1,
    "tuesday": 1,
    "wed": 2,
    "wednesday": 2,
    "w": 2,
    "thu": 3,
    "thur": 3,
    "thursday": 3,
    "th": 3,
    "fri": 4,
    "f": 4,
    "friday": 4,
    "sat": 5,
    "saturday": 5,
    "sun": 6,
    "sunday": 6
}


class GymCogError(commands.CommandError):
    pass


class Gym(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        asyncio.create_task(self.runner())

    @commands.group(brief='Gym commands',
                    invoke_without_command=True)
    async def gym(self, ctx):
        """Group for commands involving the gym."""
        await ctx.send_help(ctx.command)
    # gym register [units] [tz]

    @gym.command(help='Register as a gym member\nRequired to use any gym commands, it allows you to specify the initial settings.\nunits: Either metric (default) or imperial, the unit system in which quantities are displayed\ntz: The timezone in which times are computed, defaults to Asia/Kolkata. A list of timezones is available using `gym timezones`', usage="[units] [timezone]")
    async def register(self, ctx: commands.Context, units: str = "metric", timezone: str = "Asia/Kolkata"):
        """Register a member as a gym member."""
        if cf_common.user_db.is_gym_member(ctx.author.id):
            raise GymCogError('The user is already registered as a gym member.\n'
                              'To change units or timezone, use `gym config <tz|units> [value]`.')
        if units.lower().strip() not in ['imperial', 'metric']:
            raise GymCogError(
                'The unit should be either `metric` or `imperial`. ')

        if timezone not in zoneinfo.available_timezones():
            raise GymCogError('The timezone is not one of the available timezones.\n'
                              'To view a list of timezones, use `gym timezones`')

        cf_common.user_db.create_gym_member(
            ctx.author.id, timezone, units.lower().strip() == "imperial")
        await ctx.send(embed=discord_common.embed_success(f'Registration successful with `{units.lower().strip()}` units and timezone `{timezone}`'))

    # gym timezones [prefix]
    @gym.command(help='List all timezones/timezone prefixes\nIt allows you to list out all the possible timezones which can be used to set your user time (via `gym config tz [timezone]`, used for converting times like 4:00PM to a standard timezone visible by everyone).\npre: The timezone prefix; If not present, it lists all the prefixes (for example Asia in `Asia/Kolkata`) and if provided, it lists all the timezones starting with the prefix.', usage="[prefix]")
    async def timezones(self, ctx, pre: str = ""):
        """List all the timezones with the prefix `pre`."""
        if pre:
            tzs = [i for i in zoneinfo.available_timezones(
            ) if i.split('/')[0].lower() == pre.lower()]
            if not len(tzs):
                raise GymCogError('Timezone prefix not found\n'
                                  'List all the timezone prefixes available with `gym timezones`')

            def make_page(chunk, pi):
                title = f"Available Timezones | Page "+str(pi+1)
                embed = discord.Embed(color=discord.Color(0x00d43c))
                for j in chunk:
                    embed.add_field(name=j, value="\u200b")
                return title, embed
            paginator.paginate(self.bot, ctx.channel, [make_page(chunk, pi) for pi, chunk in enumerate(
                paginator.chunkify(tzs, 10))], wait_time=300, set_pagenum_footers=True)

            return
        timezones_prefixes = list(set(
            map(lambda x: x.split('/')[0], zoneinfo.available_timezones())))

        def make_page(chunk, pi):
            title = f"Available Timezones Prefixes | Page "+str(pi+1)
            embed = discord.Embed(color=discord.Color(0x00d43c), description='To view individual timezones, run `gym timezones <prefix>`'
                                                                             'For example, for all the timezones in Asia (such as `Asia/Kolkata` for India), run `gym timezones Asia`')
            for j in chunk:
                embed.add_field(name=j, value="\u200b")
            return title, embed
        paginator.paginate(self.bot, ctx.channel, [make_page(chunk, pi) for pi, chunk in enumerate(
            paginator.chunkify(timezones_prefixes, 10))], wait_time=300, set_pagenum_footers=True)

    @gym.group(brief='Recurring session commands',
               invoke_without_command=True)
    async def recurring(self, ctx):
        """Group for commands involving the recurring session."""
        await ctx.send_help(ctx.command)
    def tz_to_utc(self, tz: str, dt: int):
        return int(datetime.datetime.fromtimestamp(dt, tz=datetime.UTC).replace(tzinfo=zoneinfo.ZoneInfo(tz)).timestamp())
    # def utc_to_tz(tz: str, dt: int):
    #     return int(datetime.datetime.fromtimestamp(dt))
    def time_str_to_time(self, time_str: str) -> int:
        try:
            return int(dateutil.parser.parse(time_str).replace(year=1970, day=1, month=1, tzinfo=datetime.UTC).timestamp())
        except:
            raise GymCogError('Invalid time argument.\n'
                              'Use a valid time string such as `4:00PM`.')

    def datetime_to_time_str(self, time: int) -> str:
        return datetime.datetime.fromtimestamp(time, tz=datetime.UTC).strftime("%I:%M%p")

    # gym recurring schedule <day> <time>

    @recurring.command(help='Schedule a recurring gym session\nAllows you to schedule a session taking place on a certain day of the week and time\nday: The day of the week (monday, tuesday, ...)\ntime: The time at which the session takes place every week (4:00PM, 5:00AM, etc.)', name="schedule", usage="<day> <time>")
    async def recurring_schedule(self, ctx, day: str, time: str):
        """Schedule a recurring session."""
        member = cf_common.user_db.get_gym_member(ctx.author.id, ["tz"])
        if not member:
            raise GymCogError('The user is not registered as a gym member.\n'
                              'To register as a gym member, use `gym register`.')
        day_int = day_to_int.get(day.strip().lower())
        if day_int is None:
            raise GymCogError('An invalid day argument is used\n'
                              'Use day names not dates (`monday`, `tuesday`, etc.)')
        cf_common.user_db.create_recurring_session(
            ctx.author.id, day_int, self.time_str_to_time(time), member[0])
        await ctx.send(embed=discord_common.embed_success('Created recurring gym session successfully!'))
    # gym recurring list

    @recurring.command(help='List recurring gym sessions\nday: If provided, only recurring gym sessions taking place on the given day (monday, tuesday, etc.) are displayed', name="list", usage="[day]")
    async def recurring_list(self, ctx, day: str = ""):
        """List recurring events."""
        member = cf_common.user_db.get_gym_member(ctx.author.id, ["tz"])
        if not member:
            raise GymCogError('The user is not registered as a gym member.\n'
                              'To register as a gym member, use `gym register`.')
        days = ["Monday", "Tuesday", "Wednesday",
                "Thursday", "Friday", "Saturday", "Sunday"]
        if day:
            day_int = day_to_int.get(day.strip().lower())

            if day_int is None:
                raise GymCogError('An invalid day argument is used\n'
                                  'Use day names not dates (`monday`, `tuesday`, etc.)')
            sessions = cf_common.user_db.get_recurring_sessions_by_day(
                ctx.author.id, day_int, ["id", "time", "next"]).fetchmany(500)
            if not len(sessions):
                raise GymCogError('This user has not created any recurring sessions on '+days[day_int]+'.\n'
                                  'To create a recurring session, use `gym recurring schedule <day> <time>`.')

            def make_page(chunk, pi):
                title = f"Recurring Sessions on " + \
                    days[day_int]+" | Page "+str(pi+1)
                embed = discord.Embed(color=discord.Color(0x00d43c))
                for j in chunk:
                    embed.add_field(name="ID "+str(j[0]), value="Time: "+self.datetime_to_time_str(j[1])+"\nNext Session: <t:"+str(j[2])+":R>")
                return title, embed
            paginator.paginate(self.bot, ctx.channel, [make_page(chunk, pi) for pi, chunk in enumerate(
                paginator.chunkify(sessions, 10))], wait_time=300, set_pagenum_footers=True)

            return
        sessions = cf_common.user_db.get_recurring_sessions(
            ctx.author.id, ["id", "day", "time", "next"]).fetchmany(500)

        if not len(sessions):
            raise GymCogError('This user has not created any recurring sessions.\n'
                              'To create a recurring session, use `gym recurring schedule <day> <time>`.')

        def make_page(chunk, pi):
            title = f"Recurring Sessions | Page "+str(pi+1)
            embed = discord.Embed(color=discord.Color(0x00d43c))
            for j in chunk:
                embed.add_field(name="ID "+str(j[0]), value="Day: "+days[j[1]]+"\nTime: "+self.datetime_to_time_str(j[2])+"\nNext Session: <t:"+str(
                   j[3])+":R>")
            return title, embed
        paginator.paginate(self.bot, ctx.channel, [make_page(chunk, pi) for pi, chunk in enumerate(
            paginator.chunkify(sessions, 10))], wait_time=300, set_pagenum_footers=True)

    # gym recurring remove <day> <time>

    @recurring.command(help='Delete a recurring gym session\nAllows you to delete a recurring session taking place on a certain day of the week and time\nday: The day of the week (monday, tuesday, ...)\ntime: The time at which the session takes place every week (4:00PM, 5:00AM, etc.)', name="remove", usage="<day> <time>")
    async def recurring_remove(self, ctx, day: str, time: str):
        """Remove a recurring session."""
        member = cf_common.user_db.get_gym_member(ctx.author.id, ["tz"])
        if not member:
            raise GymCogError('The user is not registered as a gym member.\n'
                              'To register as a gym member, use `gym register`.')
        day_int = day_to_int.get(day.strip().lower())
        if day_int is None:
            raise GymCogError('An invalid day argument is used\n'
                              'Use day names not dates (`monday`, `tuesday`, etc.)')
        if not cf_common.user_db.remove_recurring_session(
            ctx.author.id, day_int, self.time_str_to_time(time)):
            raise GymCogError('The recurring session does not exist\n'
                              'Are you sure you provided the correct day and time?')
        await ctx.send(embed=discord_common.embed_success('Removed recurring gym session successfully!'))
    # gym recurring skip <day> <time> <reason> [n]

    @recurring.command(help='Skip recurring gym sessions\nAllows you to skip a recurring session taking place on a certain day of the week and time for a certain number of days.\nWARNING: This command will lead to a shame message being sent to all the guilds which have this bot set up which you are in\nday: The day of the week (monday, tuesday, ...)\ntime: The time at which the session takes place every week (4:00PM, 5:00AM, etc.)\nreason: The reason for the recurring session skip, will be sent in the shame message\nn: The number of sessions/weeks to be skipped, defaults to 1', name="skip", usage="<day> <time> <reason> [n]")
    async def recurring_skip(self, ctx, day: str, time: str, reason: str, n: int = 1):
        """Skip recurring sessions."""
        member = cf_common.user_db.get_gym_member(ctx.author.id, ["tz"])
        if not member:
            raise GymCogError('The user is not registered as a gym member.\n'
                              'To register as a gym member, use `gym register`.')
        day_int = day_to_int.get(day.strip().lower())
        if day_int is None:
            raise GymCogError('An invalid day argument is used\n'
                              'Use day names not dates (`monday`, `tuesday`, etc.)')
        if not cf_common.user_db.skip_recurring_session(
            ctx.author.id, day_int, self.time_str_to_time(time), n, reason, member[0]):
            raise GymCogError('The recurring session does not exist\n'
                              'Are you sure you provided the correct day and time?')
        await ctx.send(embed=discord_common.embed_success('Skipped recurring gym session successfully!'))
        await self.shame(ctx.author, "Recurring Session Skipped for "+str(n) +" Days", reason)
        
    # gym config tz [timezone]

    @gym.group(brief='Gym config commands',
               invoke_without_command=True)
    async def config(self, ctx):
        """Group for commands involving configuration."""
        await ctx.send_help(ctx.command)

    @config.command(help='Get/set user timezone\ntimezone: The timezone in which times are computed. If not present, it displays the current timezone in use. Otherwise, it sets the timezone to the given argument. A list of timezones is available using `gym timezones`\nNote: All datetimes for upcoming sessions are automagically shifted for recurring sessions to match the time in the new timezone', name="tz", usage="[timezone]")
    async def config_tz(self, ctx: commands.Context, timezone: str = ""):
        """ Get/set user timezone."""
        member = cf_common.user_db.get_gym_member(ctx.author.id, ["tz"])
        if not member:
            raise GymCogError('The user is not registered as a gym member.\n'
                              'To register as a gym member, use `gym register`.')

        if not timezone:
            await ctx.send(f"Timezone: **{member[0]}**")
            return
        if timezone not in zoneinfo.available_timezones():
            raise GymCogError('The timezone is not one of the available timezones.\n'
                              'To view a list of timezones, use `gym timezones`')

        cf_common.user_db.update_gym_member(ctx.author.id, {"tz": (member[0], timezone)})
        await ctx.send(embed=discord_common.embed_success(f'Successfully updated timezone to `{timezone}`'))

    # gym config units [units]
    @config.command(help='Get/set user units\nunits: Either metric (default) or imperial, the unit system in which quantities are displayed. If not present, it displays the current unit system in use. Otherwise, it sets the unit system to the given argument.', name="units", usage="[units]")
    async def config_units(self, ctx: commands.Context, units: str = ""):
        """ Get/set user units."""
        member = cf_common.user_db.get_gym_member(ctx.author.id, ["units"])

        if not member:
            raise GymCogError('The user is not registered as a gym member.\n'
                              'To register as a gym member, use `gym register`.')
        if not units:
            await ctx.send(f"Units: **{['metric', 'imperial'][member[0]]}**")
            return
        if units.lower().strip() not in ['imperial', 'metric']:
            raise GymCogError(
                'The unit should be either `metric` or `imperial`. ')

        cf_common.user_db.update_gym_member(
            ctx.author.id, {"units": (member[0], units.lower().strip() == "imperial")})
        await ctx.send(embed=discord_common.embed_success(f'Successfully updated units to `{units.lower().strip()}`'))

    @gym.group(brief='Exercise name commands',
               invoke_without_command=True)
    async def exercise(self, ctx):
        """Group for commands involving the exercises."""
        await ctx.send_help(ctx.command)

    # gym exercise list

    @exercise.command(help='List exercises\nThis command lists all the exercises already created by other users. To create a new exercise, use `gym exercise add <exercise name>` (use quotes around the name if it consists of multiple words)', name="list")
    async def exercise_list(self, ctx):
        """List exercises."""
        if not cf_common.user_db.is_gym_member(ctx.author.id):
            raise GymCogError('The user is not registered as a gym member.\n'
                              'To register as a gym member, use `gym register`.')

        exercises = cf_common.user_db.get_exercises().fetchmany(500)
        if not len(exercises):
            raise GymCogError('There aren\'t any exercises created.\n'
                              'To create an exercise, use `gym exercise add <exercise>`.')

        def make_page(chunk, pi):
            title = f"Exercises | Page "+str(pi+1)
            embed = discord.Embed(color=discord.Color(0x00d43c))
            for j in chunk:
                embed.add_field(name=j[0], value="\u200b")
            return title, embed
        paginator.paginate(self.bot, ctx.channel, [make_page(chunk, pi) for pi, chunk in enumerate(
            paginator.chunkify(exercises, 10))], wait_time=300, set_pagenum_footers=True)

    # gym exercise add <name>

    @exercise.command(help='Add exercise name\nThis command adds a new exercise usable in workouts. It is recommended to use `gym exercise list` to check if the given exercise is already present under another name. Use the most generic form of the name (Pushups, not Weighted Pushups)\nname: The name of the exercise to be added. Surround the name in double quotes if it consists of more than one word', name="add", usage="<name>")
    async def exercise_add(self, ctx, *args):
        """Add exercises."""
        name = ' '.join(args)
        if not cf_common.user_db.is_gym_member(ctx.author.id):
            raise GymCogError('The user is not registered as a gym member.\n'
                              'To register as a gym member, use `gym register`.')
        name = discord.utils.escape_markdown(
            discord.utils.escape_mentions(name.title()))
        try:
            cf_common.user_db.create_exercise(name)
        except sqlite3.IntegrityError:
            raise GymCogError('This exercise name already exists! You can just use it directly in `gym workout add`')
        await ctx.send(embed=discord_common.embed_success(f'Successfully added exercise!'))

    # gym exercise edit <name> <newname>

    @exercise.command(help='Edit exercise name', name="edit", hidden=True)
    @commands.has_role(constants.TLE_ADMIN)
    async def exercise_edit(self, ctx, name: str, newname: str):
        """Edit exercises."""
        name = discord.utils.escape_markdown(
            discord.utils.escape_mentions(name))
        newname = discord.utils.escape_markdown(
            discord.utils.escape_mentions(newname))

        cf_common.user_db.update_exercise(name, newname)
        await ctx.send(embed=discord_common.embed_success(f'Successfully updated exercise!'))

    # gym session list
    @gym.group(brief='Session commands',
               invoke_without_command=True)
    async def session(self, ctx):
        """Group for commands involving the gym sessions."""
        await ctx.send_help(ctx.command)

    @session.command(help='List sessions\nThis command lists all the sessions created by the user (including the next session from all the weekly recurring sessions)\nNote that dates are not categorised by status, only descending datetime.', name="list")
    async def session_list(self, ctx):
        """List sessions."""
        member = cf_common.user_db.get_gym_member(ctx.author.id, ["tz"])
        if not member:
            raise GymCogError('The user is not registered as a gym member.\n'
                              'To register as a gym member, use `gym register`.')

        sessions = cf_common.user_db.get_sessions(
            ctx.author.id, ["id", "status", "datetime"]).fetchmany(500)

        if not len(sessions):
            raise GymCogError('This user has not created any sessions.\n'
                              'To create a session, use `gym session schedule <day> <time>`.')

        def make_page(chunk, pi):
            title = f"Sessions | Page "+str(pi+1)
            embed = discord.Embed(color=discord.Color(0x00d43c))
            for j in chunk:
                if j[1].startswith("skipped"):
                    status = "Skipped ("+discord.utils.escape_markdown(
                        discord.utils.escape_mentions(j[1].split("|", 1)[1]))+")"
                else:
                    status = {
                        "complete": "Complete",
                        "inprogress": "In Progress",
                        "unresponded": "Unresponded"
                    }[j[1]]
                embed.add_field(name="ID "+str(j[0]), value="Time: <t:"+str(j[2])+":R>\nStatus: "+status)
            return title, embed
        paginator.paginate(self.bot, ctx.channel, [make_page(chunk, pi) for pi, chunk in enumerate(
            paginator.chunkify(sessions, 10))], wait_time=300, set_pagenum_footers=True)

    # gym session schedule <date> <time>
    def date_str_to_date(self, date_str: str):
        try:
            return int(dateutil.parser.parse(date_str).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=datetime.UTC).timestamp())
        except:
            raise GymCogError("Invalid date string\n"
                              "Use a format such as `DD/MM/YYYY`")

    @session.command(help='Schedule a gym session\nAllows you to schedule a session taking place on a certain date and time\ndate: The date of the session (2025/05/11, 30/11/2025, ...)\ntime: The time at which the session takes place (4:00PM, 5:00AM, etc.)', name="schedule", usage="<date> <time>")
    async def session_schedule(self, ctx, date: str, time: str):
        """Schedule a session."""
        member = cf_common.user_db.get_gym_member(ctx.author.id, ["tz"])
        if not member:
            raise GymCogError('The user is not registered as a gym member.\n'
                              'To register as a gym member, use `gym register`.')
        dt = self.tz_to_utc(member[0], self.date_str_to_date(date) + self.time_str_to_time(time))
        if datetime.datetime.now().timestamp() > dt:
            raise GymCogError('Cannot schedule a session in the past.\n'
                              'To start a session right now, use `gym start`.')
        cf_common.user_db.create_session(ctx.author.id, dt)
        await ctx.send(embed=discord_common.embed_success('Created gym session successfully starting <t:'+str(dt)+':R>!'))

    async def shame(self, user: discord.User, title: str, reason: str):
        if not user:
            return
        embed = discord.Embed(title=title, description="Offender: <@"+str(user.id)+">\nReason: "+discord.utils.escape_markdown(
                            discord.utils.escape_mentions(reason)))
        for i in self.bot.guilds:
            if i.get_member(user.id) is None:
                continue
            guild = cf_common.user_db.get_guild(i.id) 
            if not guild:
                return
            channel, role = guild
            channel = i.get_channel(channel)
            if channel:
                await channel.send(content="<@&"+str(role)+">",embed=embed)
    # gym session skip <date> <time> <reason>
    @session.command(help='Skip gym sessions\nAllows you to skip a session taking place on a certain date and time.\nIf the session is generated by a weekly recurring session, it automatically generates the next session (use `gym recurring skip` to specify the number of sessions to skip as well)\nWARNING: This command will lead to a shame message being sent to all the guilds which have this bot set up which you are in\ndate: The date of the session (2025/05/11, 30/11/2025, ...)\ntime: The time at which the session takes place (4:00PM, 5:00AM, etc.)\nreason: The reason for the session skip, will be sent in the shame message', name="skip", usage="<date> <time> <reason>")
    async def session_skip(self, ctx, date: str, time: str, *reason):
        """Skip sessions."""
        reason = ' '.join(reason)
        member = cf_common.user_db.get_gym_member(ctx.author.id, ["tz"])
        if not member:
            raise GymCogError('The user is not registered as a gym member.\n'
                              'To register as a gym member, use `gym register`.')
        dt = self.tz_to_utc(member[0], self.date_str_to_date(date) + self.time_str_to_time(time))
       
        if not cf_common.user_db.skip_session(
            ctx.author.id, dt, reason, member[0]):
            raise GymCogError('The session does not exist\n'
                              'Are you sure you provided the correct date and time?')
        await ctx.send(embed=discord_common.embed_success('Skipped gym session successfully!'))
        await self.shame(ctx.author, "Session Skipped", reason)


    # gym session start
    @session.command(help='Start gym session\nIt automatically starts an upcoming session or generates a new session at a given time\nIf a session isn\'t started within an hour of the time set, it\'s automatically skipped and sent to guilds which have a shame channel set up which you are in.\nRemember to end the session with `gym session end`!', name="start")
    async def session_start(self, ctx):
        """Start sessions."""
        member = cf_common.user_db.get_gym_member(ctx.author.id, ["tz"])
        if not member:
            raise GymCogError('The user is not registered as a gym member.\n'
                              'To register as a gym member, use `gym register`.')

        if not cf_common.user_db.start_session(ctx.author.id, member[0]):
            raise GymCogError('A gym session is already in progress!\n'
                              'Use `gym session end` to end the gym session')

        await ctx.send(embed=discord_common.embed_success('Started gym session successfully!'))

    # gym session end
    @session.command(help='End gym session\nIt ends a gym session which has been started with `gym session start`.', name="end")
    async def session_end(self, ctx):
        """End sessions."""
        member = cf_common.user_db.get_gym_member(ctx.author.id, ["tz"])
        if not member:
            raise GymCogError('The user is not registered as a gym member.\n'
                              'To register as a gym member, use `gym register`.')

        if not cf_common.user_db.end_session(ctx.author.id, member[0]):
            raise GymCogError('A gym session is not in progress!\n'
                              'Use `gym session start` to start a gym session')

        await ctx.send(embed=discord_common.embed_success('Ended gym session successfully!'))

    # gym session info <id>
    @session.command(help='Get info on a gym session\nReturns information on a session (status, workouts, etc.) with a given ID\nid: The ID of the session found using `gym session list`', name="info", usage="<id>")
    async def session_info(self, ctx, id: int):
        """Get info on sessions."""
        member = cf_common.user_db.get_gym_member(
            ctx.author.id, ["tz", "units"])
        if not member:
            raise GymCogError('The user is not registered as a gym member.\n'
                              'To register as a gym member, use `gym register`.')

        session = cf_common.user_db.get_session(
            id, ctx.author.id, ["status", "datetime"])
        if not session:
            raise GymCogError('A gym session does not exist with the given ID!\n'
                              'Use `gym session list` to list the sessions')
        if session[0].startswith("skipped"):
            status = "Skipped ("+discord.utils.escape_markdown(
                discord.utils.escape_mentions(session[0].split("|", 1)[1]))+")"
        else:
            status = {
                "complete": "Complete",
                "inprogress": "In Progress",
                "unresponded": "Unresponded"
            }[session[0]]
        if session[0] != "complete" and session[0] != "inprogress":
            embed = discord.Embed(color=discord.Color(0x00d43c), title="Session ID "+str(id), description="Time: <t:"+str(session[1])+":R>\nStatus: "+status)

            return await ctx.send(embed=embed)
        workouts = cf_common.user_db.get_workouts(
            id, ctx.author.id, ["id", "exercise", "sets", "reps", "time", "length, weight"]).fetchall()
        if not len(workouts):
            
            embed = discord.Embed(color=discord.Color(0x00d43c), title="Session ID "+str(id), description="Time: <t:"+str(session[1])+":R>\nStatus: "+status)

            return await ctx.send(embed=embed)

        def format_time(time: float):
            time, negative = abs(time), time < 0
            dt = datetime.datetime.fromtimestamp(time, tz=datetime.UTC)
            if time < 60:
                return dt.strftime("%S.%fs")
            if time < 3600:
                return dt.strftime("%Mm%S.%fs")
            if time < 3600*60:
                return dt.strftime("%Hh%Mm%S.%fs")
            return ("-"*negative) + str((dt - dt.replace(year=0, month=1, day=1)).days) + "d" + dt.strftime("%Hh%Mm%S.%fs")

        def format_length(length: float):
            length, negative = abs(length), length < 0

            if member[1]:
                if length*39.3701 < 12:
                    return ("-"*negative) + str(round(length*39.3701, 1))+"in"
                elif length*39.3701 < 12*3:
                    return ("-"*negative) + str(round(length*39.3701/12, 1))+"ft"
                elif length*39.3701 < 12 * 3 * 1760:
                    return ("-"*negative) + str(round(length*39.3701/(12*3), 1))+"yd"
                else:
                    return ("-"*negative) + str(round(length*39.3701/(12*3*1760), 1))+"mi"

            if length*1000 < 10:
                return ("-"*negative) + str(round(length*1000, 1))+"mm"
            elif length*100 < 100:
                return ("-"*negative) + str(round(length*100, 1))+"cm"
            elif length < 1000:
                return ("-"*negative) + str(round(length, 1))+"m"
            return ("-"*negative) + str(round(length/1000, 1))+"km"

        def format_weight(weight: float):
            weight, negative = abs(weight), weight < 0

            if member[1]:
                if weight*35.274 < 16:
                    return ("-"*negative) + str(round(weight*35.274, 1))+"oz"
                else:
                    return ("-"*negative) + str(round(weight*35.274/16, 1))+"lb"

            if weight < 1:
                return ("-"*negative) + str(round(weight*1000, 1))+"g"
            return ("-"*negative) + str(round(weight, 1))+"kg"

        def make_page(chunk, pi):
            title = f"Session ID "+str(id)+" | Page "+str(pi+1)
            embed = discord.Embed(color=discord.Color(0x00d43c), description="Time: <t:"+str(session[1])+":R>\nStatus: "+status)
            for j in chunk:
                workout = "Exercise: "+j[1]+"\nSets: " + \
                    str(int(j[2]))+"\nReps: "+str(int(j[3]))
                if j[4] is not None:
                    workout += "\nTime: "+format_time(j[4])
                if j[5] is not None:
                    workout += "\nLength: "+format_length(j[5])
                if j[6] is not None:
                    workout += "\nWeight: "+format_weight(j[6])
                embed.add_field(name="ID "+str(j[0]), value=workout)
            return title, embed
        paginator.paginate(self.bot, ctx.channel, [make_page(chunk, pi) for pi, chunk in enumerate(
            paginator.chunkify(workouts, 10))], wait_time=300, set_pagenum_footers=True)

    # gym session workout add <id> <name> <sets> <reps> [amount] [amount2] [amount3]
    @session.group(brief='Workout commands',
                   invoke_without_command=True)
    async def workout(self, ctx):
        """Group for commands involving the session workouts."""
        await ctx.send_help(ctx.command)

    @workout.command(help='Add a workout to a session\nA "workout" in this bot is an instance of an exercise being done in a particular session (so each session has multiple workouts, such as Pushups, Pullups, etc.)\nThis command adds a workout with a certain number of sets and reps with certain quantities associated (weight, length and time).\nid: The ID of the session (found with `gym session list`)\nname: The name of the exercise (found using `gym exercise list`). If the exercise name consists of two or more words, surround it with double quotes\nsets: The number of sets of the exercise\nreps: The number of reps per set\namounts: The (optional) amounts associated with the exercise. For example, if you did pushups with 10kg additional weight, you would add "10kg". If you did weighted planks for 1min with 10kg, you add "60s 10kg". Counterweights/assist weights should be set as negative.', name="add", usage="<id> <name> <sets> <reps> [...amounts]")
    async def workout_add(self, ctx, id: str, name: str, sets: int, reps: int, *amounts):
        """Add a workout."""
        member = cf_common.user_db.get_gym_member(ctx.author.id, ["tz"])
        if not member:
            raise GymCogError('The user is not registered as a gym member.\n'
                              'To register as a gym member, use `gym register`.')
        name = discord.utils.escape_markdown(
            discord.utils.escape_mentions(name.title()))

        data = {
            "time": None,
            "weight": None,
            "length": None
        }

        units = {
            's': ("time", 1),
            'min': ("time", 60),
            'h': ("time", 3600),

            'lb': ("weight", 0.453592),
            'oz': ("weight", 0.0283495),
            'in': ("length", 0.0254),
            'ft': ("length", 0.3048),
            'yd': ("length", 0.9144),
            'mi': ("length", 1609.34),

            'kg': ("weight", 1),
            'g': ("weight", 0.001),

            'mm': ("length", 0.001),
            'cm': ("length", 0.01),
            'km': ("length", 1000),
            'm': ("length", 1),
        }
        for i in amounts:
            k = False
            for j in units:
                if i.endswith(j):
                    try:
                        v = float(i[:-len(j)])
                    except:
                        raise GymCogError('Invalid amount value.\n'
                                          'Use a format such as `10.2kg`.')
                    data[units[j][0]] = v * units[j][1]
                    k = True
                    break
            if not k:
                raise GymCogError('Unit not found.\n'
                                  'Use a format such as `10.2kg`.')
        session = cf_common.user_db.get_session(id, ctx.author.id, ["status"])
        if not session:
            raise GymCogError('Session ID not found!\n'
                              'Use `gym session list` to get the sessions')
        if session[0] != "complete" and session[0] != "inprogress":
            raise GymCogError('The session has not been completed/started!\n'
                              'Use `gym session start` to start a session')
        try:
            cf_common.user_db.add_workout(
                ctx.author.id, id, name, sets, reps, data["time"], data["weight"], data["length"])
        except sqlite3.IntegrityError:
            raise GymCogError('The exercise name does not exist!\n'
                              'Are you sure you used the correct name (and surrounded it with double quotes if it\'s a multiword name)?')
        
        await ctx.send(embed=discord_common.embed_success('Added gym workout successfully!'))

    # gym session workout remove <id>

    @workout.command(help='Remove a workout from a session\nid: The ID of the workout (found using `gym session info`)', name="remove", usage="<id>")
    async def workout_remove(self, ctx, id: str):
        """Remove a workout."""
        if not cf_common.user_db.is_gym_member(ctx.author.id):
            raise GymCogError('The user is not registered as a gym member.\n'
                              'To register as a gym member, use `gym register`.')

        if not cf_common.user_db.remove_workout(id, ctx.author.id):
            raise GymCogError("This workout does not exist!"
                              "Are you sure you used the correct ID (remember to use the workout ID found in `gym session info`, not the session ID)?")

        await ctx.send(embed=discord_common.embed_success('Removed gym workout successfully!'))

    # gym records [exercise]
    @gym.command(help='Get a list of exercise records\nGet a list of records for each exercise.\nexercise: If the exercise is provided, more detailed information is provided for one specific exercise records, else all the records are provided.', usage="[exercise]")
    async def records(self, ctx, *exercise):
        exercise = ' '.join(exercise) 
        """List exercise records."""
        member = cf_common.user_db.get_gym_member(ctx.author.id, ["tz", "units"])
        if not member:
            raise GymCogError('The user is not registered as a gym member.\n'
                              'To register as a gym member, use `gym register`.')
        def format_time(time: float):
            time, negative = abs(time), time < 0
            dt = datetime.datetime.fromtimestamp(time, tz=datetime.UTC)
            if time < 60:
                return dt.strftime("%S.%fs")
            if time < 3600:
                return dt.strftime("%Mm%S.%fs")
            if time < 3600*60:
                return dt.strftime("%Hh%Mm%S.%fs")
            return ("-"*negative) + str((dt - dt.replace(year=0, month=1, day=1)).days) + "d" + dt.strftime("%Hh%Mm%S.%fs")

        def format_length(length: float):
            length, negative = abs(length), length < 0

            if member[1]:
                if length*39.3701 < 12:
                    return ("-"*negative) + str(round(length*39.3701, 1))+"in"
                elif length*39.3701 < 12*3:
                    return ("-"*negative) + str(round(length*39.3701/12, 1))+"ft"
                elif length*39.3701 < 12 * 3 * 1760:
                    return ("-"*negative) + str(round(length*39.3701/(12*3), 1))+"yd"
                else:
                    return ("-"*negative) + str(round(length*39.3701/(12*3*1760), 1))+"mi"

            if length*1000 < 10:
                return ("-"*negative) + str(round(length*1000, 1))+"mm"
            elif length*100 < 100:
                return ("-"*negative) + str(round(length*100, 1))+"cm"
            elif length < 1000:
                return ("-"*negative) + str(round(length, 1))+"m"
            return ("-"*negative) + str(round(length/1000, 1))+"km"

        def format_weight(weight: float):
            weight, negative = abs(weight), weight < 0

            if member[1]:
                if weight*35.274 < 16:
                    return ("-"*negative) + str(round(weight*35.274, 1))+"oz"
                else:
                    return ("-"*negative) + str(round(weight*35.274/16, 1))+"lb"

            if weight < 1:
                return ("-"*negative) + str(round(weight*1000, 1))+"g"
            return ("-"*negative) + str(round(weight, 1))+"kg"
        if not exercise:
            records = cf_common.user_db.get_records(["exercise", "type", "amount"]).fetchmany(2500)
            records_dict = {}
            for i in records:
                if i[2] is None:
                    continue
                records_dict[i[0]] = records_dict.get(i[0], {})
                records_dict[i[0]][i[1]] = i[2]
            
            if not len(records):
                raise GymCogError('No records are available!\n'
                                  'Add some workouts with `gym session workout add`.')
            def make_page(chunk, pi):
                title = f"Records | Page "+str(pi+1)
                embed = discord.Embed(color=discord.Color(0x00d43c))
                
                for j in chunk:
                    functions = {
                        "sets": lambda x : str(int(x)),
                        "reps": lambda x : str(int(x)),
                        "time": format_time,
                        "length": format_length,
                        "weight": format_weight
                    }
                    record = ""
                    for i in records_dict[j]:
                        record+=i.title() + ": "+functions[i](records_dict[j][i])+"\n"
                    embed.add_field(name=j, value=record)
                return title, embed
            return paginator.paginate(self.bot, ctx.channel, [make_page(chunk, pi) for pi, chunk in enumerate(
                paginator.chunkify(list(records_dict), 10))], wait_time=300, set_pagenum_footers=True)
        exercise = discord.utils.escape_markdown(
            discord.utils.escape_mentions(exercise.title()))
        
        records = cf_common.user_db.get_records_for_exercise(exercise).fetchmany(2500)
        if not len(records):
            raise GymCogError('No records available for the exercise!\n'
                                'Are you sure you spelled the exercise name correctly?')
        embed = discord.Embed(title="Records for " + exercise, color=discord.Color(0x00d43c))
        for j in records:
            functions = {
                "sets": lambda x : str(int(x)),
                "reps": lambda x : str(int(x)),
                "time": format_time,
                "length": format_length,
                "weight": format_weight
            }
            embed.add_field(name=j[0].title(), value="Value: "+functions[j[0]](j[1])+"\nAchiever: <@"+str(j[2])+">\nDate: <t:"+str(j[3])+">")
        await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())
    # gym setup <channel> <role>
    @gym.command(help='Setup shaming channel and role', hidden=True)
    @commands.has_role(constants.TLE_ADMIN)
    async def setup(self, ctx, channel: discord.TextChannel, role: discord.Role):
        if not ctx.guild.get_channel(channel.id):
            raise GymCogError("Cannot find the channel!\n"
                               "Are you sure the bot has access to the channel?")
        cf_common.user_db.setup_guild(ctx.guild.id, channel.id, role.id)
        await ctx.send(embed=discord_common.embed_success("Successfully set server up!"))

    @discord_common.send_error_if(GymCogError)
    async def cog_command_error(self, ctx, error):
        pass
    
    async def runner(self):
        while True:
            await asyncio.sleep(60)
            for i in cf_common.user_db.get_incomplete_sessions():
                await self.shame(self.bot.get_user(i[0]), "Skipped Session", "Did not respond in time")
            for i in cf_common.user_db.get_close_sessions():
                if not self.bot.get_user(i[0]):
                    continue
                await self.bot.get_user(i[0]).send("Reminder! Your gym session starts <t:"+str(i[1])+":R>")
            for i in cf_common.user_db.get_open_sessions():
                if not self.bot.get_user(i[0]):
                    continue
                await self.bot.get_user(i[0]).send("Your session was auto-completed for being in progress longer than 3 days")
            cf_common.user_db.fix_recurring_sessions()

async def setup(bot):
    await bot.add_cog(Gym(bot))
