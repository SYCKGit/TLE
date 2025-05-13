import sqlite3
import datetime
import zoneinfo
import secrets
from enum import IntEnum
from collections import namedtuple

from discord.ext import commands

from tle.util import codeforces_api as cf, paginator
from tle.util import codeforces_common as cf_common

_DEFAULT_VC_RATING = 1500

class Gitgud(IntEnum):
    GOTGUD = 0
    GITGUD = 1
    NOGUD = 2
    FORCED_NOGUD = 3

class Training(IntEnum):
    NOTSTARTED = 0
    ACTIVE = 1
    COMPLETED = 2

class TrainingProblemStatus(IntEnum):
    SOLVED = 0
    SOLVED_TOO_SLOW = 1
    ACTIVE = 2
    SKIPPED = 3
    INVALIDATED = 4

class Duel(IntEnum):
    PENDING = 0
    DECLINED = 1
    WITHDRAWN = 2
    EXPIRED = 3
    ONGOING = 4
    COMPLETE = 5
    INVALID = 6

class Winner(IntEnum):
    DRAW = 0
    CHALLENGER = 1
    CHALLENGEE = 2

class DuelType(IntEnum):
    UNOFFICIAL = 0
    OFFICIAL = 1
    ADJUNOFFICIAL = 2
    ADJOFFICIAL = 3

class RatedVC(IntEnum):
    ONGOING = 0
    FINISHED = 1


class UserDbError(commands.CommandError):
    pass


class DatabaseDisabledError(UserDbError):
    pass


class DummyUserDbConn:
    def __getattribute__(self, item):
        raise DatabaseDisabledError


class UniqueConstraintFailed(UserDbError):
    pass


def namedtuple_factory(cursor, row):
    """Returns sqlite rows as named tuples."""
    fields = [col[0] for col in cursor.description if col[0].isidentifier()]
    Row = namedtuple("Row", fields)
    return Row(*row)


class UserDbConn:
    role_cache: dict[tuple[int, str], int]

    def __init__(self, dbfile):
        self.conn = sqlite3.connect(dbfile)
        self.conn.row_factory = namedtuple_factory
        self.role_cache = {}
        self.guild_cache = {}
        self.create_tables()
        self.populate_cache()

    def create_tables(self):
        self.conn.execute('PRAGMA foreign_keys = ON;')

        self.conn.execute(
            'CREATE TABLE IF NOT EXISTS user_handle ('
            'user_id     TEXT,'
            'guild_id    TEXT,'
            'handle      TEXT,'
            'active      INTEGER,'
            'PRIMARY KEY (user_id, guild_id)'
            ')'
        )
        self.conn.execute('CREATE UNIQUE INDEX IF NOT EXISTS ix_user_handle_guild_handle '
                          'ON user_handle (guild_id, handle)')
        self.conn.execute(
            'CREATE TABLE IF NOT EXISTS cf_user_cache ('
            'handle              TEXT PRIMARY KEY,'
            'first_name          TEXT,'
            'last_name           TEXT,'
            'country             TEXT,'
            'city                TEXT,'
            'organization        TEXT,'
            'contribution        INTEGER,'
            'rating              INTEGER,'
            'maxRating           INTEGER,'
            'last_online_time    INTEGER,'
            'registration_time   INTEGER,'
            'friend_of_count     INTEGER,'
            'title_photo         TEXT'
            ')'
        )
        # TODO: Make duel tables guild-aware.
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS duelist(
                "user_id"	INTEGER PRIMARY KEY NOT NULL,
                "rating"	INTEGER NOT NULL,
                "guild_id"  TEXT
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS duel(
                "id"	INTEGER PRIMARY KEY AUTOINCREMENT,
                "challenger"	INTEGER NOT NULL,
                "challengee"	INTEGER NOT NULL,
                "issue_time"	REAL NOT NULL,
                "start_time"	REAL,
                "finish_time"	REAL,
                "problem_name"	TEXT,
                "contest_id"	INTEGER,
                "p_index"	INTEGER,
                "status"	INTEGER,
                "winner"	INTEGER,
                "type"		INTEGER,
                "guild_id"  TEXT
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS duel_settings (
                guild_id TEXT PRIMARY KEY,
                channel_id TEXT
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS "challenge" (
                "id"	INTEGER PRIMARY KEY AUTOINCREMENT,
                "user_id"	TEXT NOT NULL,
                "issue_time"	REAL NOT NULL,
                "finish_time"	REAL,
                "problem_name"	TEXT NOT NULL,
                "contest_id"	INTEGER NOT NULL,
                "p_index"	INTEGER NOT NULL,
                "rating_delta"	INTEGER NOT NULL,
                "status"	INTEGER NOT NULL
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS "user_challenge" (
                "user_id"	TEXT,
                "active_challenge_id"	INTEGER,
                "issue_time"	REAL,
                "score"	INTEGER NOT NULL,
                "num_completed"	INTEGER NOT NULL,
                "num_skipped"	INTEGER NOT NULL,
                PRIMARY KEY("user_id")
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS reminder (
                guild_id TEXT PRIMARY KEY,
                channel_id TEXT,
                role_id TEXT,
                before TEXT
            )
        ''')
        self.conn.execute(
            'CREATE TABLE IF NOT EXISTS starboard ('
            'guild_id     TEXT PRIMARY KEY,'
            'channel_id   TEXT'
            ')'
        )
        self.conn.execute(
            'CREATE TABLE IF NOT EXISTS starboard_message ('
            'original_msg_id    TEXT PRIMARY KEY,'
            'starboard_msg_id   TEXT,'
            'guild_id           TEXT'
            ')'
        )
        self.conn.execute(
            'CREATE TABLE IF NOT EXISTS rankup ('
            'guild_id     TEXT PRIMARY KEY,'
            'channel_id   TEXT'
            ')'
        )
        self.conn.execute(
            'CREATE TABLE IF NOT EXISTS auto_role_update ('
            'guild_id     TEXT PRIMARY KEY'
            ')'
        )

        # Rated VCs stuff:
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS "rated_vcs" (
                "id"	         INTEGER PRIMARY KEY AUTOINCREMENT,
                "contest_id"     INTEGER NOT NULL,
                "start_time"     REAL,
                "finish_time"    REAL,
                "status"         INTEGER,
                "guild_id"       TEXT
            )
        ''')

        # TODO: Do we need to explicitly specify the fk constraint or just depend on the middleware?
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS "rated_vc_users" (
                "vc_id"	         INTEGER,
                "user_id"        TEXT NOT NULL,
                "rating"         INTEGER,

                CONSTRAINT fk_vc
                    FOREIGN KEY (vc_id)
                    REFERENCES rated_vcs(id),

                PRIMARY KEY(vc_id, user_id)
            )
        ''')

        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS rated_vc_settings (
                guild_id TEXT PRIMARY KEY,
                channel_id TEXT
            )
        ''')

        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS training_settings (
                guild_id TEXT PRIMARY KEY,
                channel_id TEXT
            )
        ''')

        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS trainings (
                "id"	INTEGER PRIMARY KEY AUTOINCREMENT,
                "user_id" TEXT,
                "score" INTEGER,
                "lives" INTEGER,
                "time_left"     REAL,
                "mode"  INTEGER NOT NULL,
                "status" INTEGER NOT NULL
            )
        ''')

        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS training_problems (
                "id"	INTEGER PRIMARY KEY AUTOINCREMENT,
                "training_id"   INTEGER NOT NULL,
                "issue_time"	REAL NOT NULL,
                "finish_time"	REAL,
                "problem_name"	TEXT NOT NULL,
                "contest_id"	INTEGER NOT NULL,
                "p_index"	INTEGER NOT NULL,
                "rating"	INTEGER NOT NULL,
                "status"	INTEGER NOT NULL
            )
        ''')

        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS round_settings (
                guild_id TEXT PRIMARY KEY,
                channel_id TEXT
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS lockout_ongoing_rounds (
                "id"	INTEGER PRIMARY KEY AUTOINCREMENT,
                "guild" TEXT,
                "users" TEXT,
                "rating" TEXT,
                "points" TEXT,
                "time" INT,
                "problems" TEXT,
                "status" TEXT,
                "duration" INTEGER,
                "repeat" INTEGER,
                "times" TEXT
            )
        ''')

        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS lockout_finished_rounds(
                "id"	INTEGER PRIMARY KEY AUTOINCREMENT,
                "guild" TEXT,
                "users" TEXT,
                "rating" TEXT,
                "points" TEXT,
                "time" INT,
                "problems" TEXT,
                "status" TEXT,
                "duration" INTEGER,
                "repeat" INTEGER,
                "times" TEXT,
                "end_time" INT
            )
            ''')

        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS role_reactions (
                message_id INTEGER,
                role_id INTEGER,
                emoji TEXT,
                PRIMARY KEY(message_id, emoji)
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS gym_members (
                discord_id INTEGER PRIMARY KEY NOT NULL, 
                units INTEGER DEFAULT FALSE NOT NULL,
                tz TEXT DEFAULT "Asia/Kolkata" NOT NULL
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS gym_exercises (
                name STRING PRIMARY KEY
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS gym_records (
                exercise TEXT NOT NULL REFERENCES gym_exercises ON UPDATE CASCADE,
                type TEXT NOT NULL, 
                amount REAL NOT NULL,
                workout INTEGER NOT NULL REFERENCES gym_workouts,
                PRIMARY KEY(exercise, type)
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS gym_recurring_sessions (
                id INTEGER PRIMARY KEY,
                user INTEGER NOT NULL REFERENCES gym_members,
                day INTEGER NOT NULL,
                time INTEGER NOT NULL,
                next INTEGER NOT NULL
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS gym_sessions (
                id INTEGER PRIMARY KEY,
                user INTEGER NOT NULL REFERENCES gym_members,
                datetime INTEGER NOT NULL,
                status TEXT NOT NULL
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS gym_workouts (
                id INTEGER PRIMARY KEY,
                user INTEGER NOT NULL REFERENCES gym_members,
                session INTEGER NOT NULL REFERENCES gym_sessions,
                exercise TEXT NOT NULL REFERENCES gym_exercises ON UPDATE CASCADE,
                sets INTEGER NOT NULL,
                reps INTEGER NOT NULL,
                time REAL,
                weight REAL,
                length REAL
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS gym_guilds (
                guild INTEGER PRIMARY KEY,
                channel INTEGER,
                role INTEGER
            )
        ''')
        
        
        
    def populate_cache(self):
        self.role_cache.clear()
        query = 'SELECT message_id, emoji, role_id FROM role_reactions'
        for message_id, emoji, role_id in self.conn.execute(query).fetchall():
            self.role_cache[(message_id, emoji)] = role_id

    # Helper functions.

    def _insert_one(self, table: str, columns, values: tuple):
        n = len(values)
        query = '''
            INSERT OR REPLACE INTO {} ({}) VALUES ({})
        '''.format(table, ', '.join(columns), ', '.join(['?'] * n))
        rc = self.conn.execute(query, values).rowcount
        self.conn.commit()
        return rc

    def _insert_many(self, table: str, columns, values: list):
        n = len(columns)
        query = '''
            INSERT OR REPLACE INTO {} ({}) VALUES ({})
        '''.format(table, ', '.join(columns), ', '.join(['?'] * n))
        rc = self.conn.executemany(query, values).rowcount
        self.conn.commit()
        return rc

    def _fetchone(self, query: str, params=None, row_factory=None):
        self.conn.row_factory = row_factory
        res = self.conn.execute(query, params).fetchone()
        self.conn.row_factory = None
        return res

    def _fetchall(self, query: str, params=None, row_factory=None):
        self.conn.row_factory = row_factory
        res = self.conn.execute(query, params).fetchall()
        self.conn.row_factory = None
        return res

    def new_challenge(self, user_id, issue_time, prob, delta):
        query1 = '''
            INSERT INTO challenge
            (user_id, issue_time, problem_name, contest_id, p_index, rating_delta, status)
            VALUES
            (?, ?, ?, ?, ?, ?, 1)
        '''
        query2 = '''
            INSERT OR IGNORE INTO user_challenge (user_id, score, num_completed, num_skipped)
            VALUES (?, 0, 0, 0)
        '''
        query3 = '''
            UPDATE user_challenge SET active_challenge_id = ?, issue_time = ?
            WHERE user_id = ? AND active_challenge_id IS NULL
        '''
        cur = self.conn.cursor()
        cur.execute(query1, (user_id, issue_time, prob.name, prob.contestId, prob.index, delta))
        last_id, rc = cur.lastrowid, cur.rowcount
        if rc != 1:
            self.conn.rollback()
            return 0
        cur.execute(query2, (user_id,))
        cur.execute(query3, (last_id, issue_time, user_id))
        if cur.rowcount != 1:
            self.conn.rollback()
            return 0
        self.conn.commit()
        return 1

    def check_challenge(self, user_id):
        query1 = '''
            SELECT active_challenge_id, issue_time FROM user_challenge
            WHERE user_id = ?
        '''
        res = self.conn.execute(query1, (user_id,)).fetchone()
        if res is None: return None
        c_id, issue_time = res
        query2 = '''
            SELECT problem_name, contest_id, p_index, rating_delta FROM challenge
            WHERE id = ?
        '''
        res = self.conn.execute(query2, (c_id,)).fetchone()
        if res is None: return None
        return c_id, issue_time, res[0], res[1], res[2], res[3]

    def get_gudgitters_last(self, timestamp):
        query = '''
            SELECT user_id, rating_delta FROM challenge WHERE finish_time >= ? ORDER BY user_id
        '''
        return self.conn.execute(query, (timestamp,)).fetchall()

    def get_gudgitters_timerange(self, timestampStart, timestampEnd):
        query = '''
            SELECT user_id, rating_delta, issue_time FROM challenge WHERE finish_time >= ? AND finish_time <= ? ORDER BY user_id
        '''
        return self.conn.execute(query, (timestampStart,timestampEnd)).fetchall()

    def get_gudgitters(self):
        query = '''
            SELECT user_id, score FROM user_challenge
        '''
        return self.conn.execute(query).fetchall()

    def howgud(self, user_id):
        query = '''
            SELECT rating_delta FROM challenge WHERE user_id = ? AND finish_time IS NOT NULL
        '''
        return self.conn.execute(query, (user_id,)).fetchall()

    def get_noguds(self, user_id):
        query = ('SELECT problem_name '
                 'FROM challenge '
                 f'WHERE user_id = ? AND status = {Gitgud.NOGUD}')
        return {name for name, in self.conn.execute(query, (user_id,)).fetchall()}

    def gitlog(self, user_id):
        query = f'''
            SELECT issue_time, finish_time, problem_name, contest_id, p_index, rating_delta, status
            FROM challenge WHERE user_id = ? AND status != {Gitgud.FORCED_NOGUD} ORDER BY issue_time DESC
        '''
        return self.conn.execute(query, (user_id,)).fetchall()

    def complete_challenge(self, user_id, challenge_id, finish_time, delta):
        query1 = f'''
            UPDATE challenge SET finish_time = ?, status = {Gitgud.GOTGUD}
            WHERE id = ? AND status = {Gitgud.GITGUD}
        '''
        query2 = '''
            UPDATE user_challenge SET score = score + ?, num_completed = num_completed + 1,
            active_challenge_id = NULL, issue_time = NULL
            WHERE user_id = ? AND active_challenge_id = ?
        '''
        rc = self.conn.execute(query1, (finish_time, challenge_id)).rowcount
        if rc != 1:
            self.conn.rollback()
            return 0
        rc = self.conn.execute(query2, (delta, user_id, challenge_id)).rowcount
        if rc != 1:
            self.conn.rollback()
            return 0
        self.conn.commit()
        return 1

    def skip_challenge(self, user_id, challenge_id, status):
        query1 = '''
            UPDATE user_challenge SET active_challenge_id = NULL, issue_time = NULL
            WHERE user_id = ? AND active_challenge_id = ?
        '''
        query2 = f'''
            UPDATE challenge SET status = ? WHERE id = ? AND status = {Gitgud.GITGUD}
        '''
        rc = self.conn.execute(query1, (user_id, challenge_id)).rowcount
        if rc != 1:
            self.conn.rollback()
            return 0
        rc = self.conn.execute(query2, (status, challenge_id)).rowcount
        if rc != 1:
            self.conn.rollback()
            return 0
        self.conn.commit()
        return 1

    def cache_cf_user(self, user):
        query = ('INSERT OR REPLACE INTO cf_user_cache '
                 '(handle, first_name, last_name, country, city, organization, contribution, '
                 '    rating, maxRating, last_online_time, registration_time, friend_of_count, title_photo) '
                 'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)')
        with self.conn:
            return self.conn.execute(query, user).rowcount

    def fetch_cf_user(self, handle):
        query = ('SELECT handle, first_name, last_name, country, city, organization, contribution, '
                 '    rating, maxRating, last_online_time, registration_time, friend_of_count, title_photo '
                 'FROM cf_user_cache '
                 'WHERE UPPER(handle) = UPPER(?)')
        user = self.conn.execute(query, (handle,)).fetchone()
        return cf_common.fix_urls(cf.User._make(user)) if user else None

    def set_handle(self, user_id, guild_id, handle):
        query = ('SELECT user_id '
                 'FROM user_handle '
                 'WHERE guild_id = ? AND handle = ?')
        existing = self.conn.execute(query, (guild_id, handle)).fetchone()
        if existing and int(existing[0]) != user_id:
            raise UniqueConstraintFailed

        query = ('INSERT OR REPLACE INTO user_handle '
                 '(user_id, guild_id, handle, active) '
                 'VALUES (?, ?, ?, 1)')
        with self.conn:
            return self.conn.execute(query, (user_id, guild_id, handle)).rowcount

    def set_inactive(self, guild_id_user_id_pairs):
        query = ('UPDATE user_handle '
                 'SET active = 0 '
                 'WHERE guild_id = ? AND user_id = ?')
        with self.conn:
            return self.conn.executemany(query, guild_id_user_id_pairs).rowcount

    def get_handle(self, user_id, guild_id):
        query = ('SELECT handle '
                 'FROM user_handle '
                 'WHERE user_id = ? AND guild_id = ?')
        res = self.conn.execute(query, (user_id, guild_id)).fetchone()
        return res[0] if res else None

    def get_user_id(self, handle, guild_id):
        query = ('SELECT user_id '
                 'FROM user_handle '
                 'WHERE UPPER(handle) = UPPER(?) AND guild_id = ?')
        res = self.conn.execute(query, (handle, guild_id)).fetchone()
        return int(res[0]) if res else None

    def remove_handle(self, handle, guild_id):
        query = ('DELETE FROM user_handle '
                 'WHERE UPPER(handle) = UPPER(?) AND guild_id = ?')
        with self.conn:
            return self.conn.execute(query, (handle, guild_id)).rowcount

    def get_handles_for_guild(self, guild_id):
        query = ('SELECT user_id, handle '
                 'FROM user_handle '
                 'WHERE guild_id = ? AND active = 1')
        res = self.conn.execute(query, (guild_id,)).fetchall()
        return [(int(user_id), handle) for user_id, handle in res]

    def get_cf_users_for_guild(self, guild_id):
        query = ('SELECT u.user_id, c.handle, c.first_name, c.last_name, c.country, c.city, '
                 '    c.organization, c.contribution, c.rating, c.maxRating, c.last_online_time, '
                 '    c.registration_time, c.friend_of_count, c.title_photo '
                 'FROM user_handle AS u '
                 'LEFT JOIN cf_user_cache AS c '
                 'ON u.handle = c.handle '
                 'WHERE u.guild_id = ? AND u.active = 1')
        res = self.conn.execute(query, (guild_id,)).fetchall()
        return [(int(t[0]), cf.User._make(t[1:])) for t in res]

    def get_reminder_settings(self, guild_id):
        query = '''
            SELECT channel_id, role_id, before
            FROM reminder
            WHERE guild_id = ?
        '''
        return self.conn.execute(query, (guild_id,)).fetchone()

    def set_reminder_settings(self, guild_id, channel_id, role_id, before):
        query = '''
            INSERT OR REPLACE INTO reminder (guild_id, channel_id, role_id, before)
            VALUES (?, ?, ?, ?)
        '''
        self.conn.execute(query, (guild_id, channel_id, role_id, before))
        self.conn.commit()

    def clear_reminder_settings(self, guild_id):
        query = '''DELETE FROM reminder WHERE guild_id = ?'''
        self.conn.execute(query, (guild_id,))
        self.conn.commit()

    def get_starboard(self, guild_id):
        query = ('SELECT channel_id '
                 'FROM starboard '
                 'WHERE guild_id = ?')
        return self.conn.execute(query, (guild_id,)).fetchone()

    def set_starboard(self, guild_id, channel_id):
        query = ('INSERT OR REPLACE INTO starboard '
                 '(guild_id, channel_id) '
                 'VALUES (?, ?)')
        self.conn.execute(query, (guild_id, channel_id))
        self.conn.commit()

    def clear_starboard(self, guild_id):
        query = ('DELETE FROM starboard '
                 'WHERE guild_id = ?')
        self.conn.execute(query, (guild_id,))
        self.conn.commit()

    def add_starboard_message(self, original_msg_id, starboard_msg_id, guild_id):
        query = ('INSERT INTO starboard_message '
                 '(original_msg_id, starboard_msg_id, guild_id) '
                 'VALUES (?, ?, ?)')
        self.conn.execute(query, (original_msg_id, starboard_msg_id, guild_id))
        self.conn.commit()

    def check_exists_starboard_message(self, original_msg_id):
        query = ('SELECT 1 '
                 'FROM starboard_message '
                 'WHERE original_msg_id = ?')
        res = self.conn.execute(query, (original_msg_id,)).fetchone()
        return res is not None

    def remove_starboard_message(self, *, original_msg_id=None, starboard_msg_id=None):
        assert (original_msg_id is None) ^ (starboard_msg_id is None)
        if original_msg_id is not None:
            query = ('DELETE FROM starboard_message '
                     'WHERE original_msg_id = ?')
            rc = self.conn.execute(query, (original_msg_id,)).rowcount
        else:
            query = ('DELETE FROM starboard_message '
                     'WHERE starboard_msg_id = ?')
            rc = self.conn.execute(query, (starboard_msg_id,)).rowcount
        self.conn.commit()
        return rc

    def clear_starboard_messages_for_guild(self, guild_id):
        query = ('DELETE FROM starboard_message '
                 'WHERE guild_id = ?')
        rc = self.conn.execute(query, (guild_id,)).rowcount
        self.conn.commit()
        return rc

    def set_duel_channel(self, guild_id, channel_id):
        query = ('INSERT OR REPLACE INTO duel_settings '
                 ' (guild_id, channel_id) VALUES (?, ?)'
                 )
        with self.conn:
            self.conn.execute(query, (guild_id, channel_id))

    def get_duel_channel(self, guild_id):
        query = ('SELECT channel_id '
                 'FROM duel_settings '
                 'WHERE guild_id = ?')
        channel_id = self.conn.execute(query, (guild_id,)).fetchone()
        return int(channel_id[0]) if channel_id else None

    def check_duel_challenge(self, userid, guild_id):
        query = f'''
            SELECT id FROM duel
            WHERE (challengee = ? OR challenger = ?) AND guild_id = ? AND (status == {Duel.ONGOING} OR status == {Duel.PENDING})
        '''
        return self.conn.execute(query, (userid, userid, guild_id)).fetchone()

    def check_duel_accept(self, challengee, guild_id):
        query = f'''
            SELECT id, challenger, problem_name FROM duel
            WHERE challengee = ? AND guild_id = ? AND status == {Duel.PENDING}
        '''
        return self.conn.execute(query, (challengee,guild_id)).fetchone()

    def check_duel_decline(self, challengee, guild_id):
        query = f'''
            SELECT id, challenger FROM duel
            WHERE challengee = ? AND guild_id = ? AND status == {Duel.PENDING}
        '''
        return self.conn.execute(query, (challengee,guild_id)).fetchone()

    def check_duel_withdraw(self, challenger, guild_id):
        query = f'''
            SELECT id, challengee FROM duel
            WHERE challenger = ? AND guild_id = ? AND status == {Duel.PENDING}
        '''
        return self.conn.execute(query, (challenger,guild_id)).fetchone()

    def check_duel_draw(self, userid, guild_id):
        query = f'''
            SELECT id, challenger, challengee, start_time, type FROM duel
            WHERE (challenger = ? OR challengee = ?) AND guild_id = ? AND status == {Duel.ONGOING}
        '''
        return self.conn.execute(query, (userid, userid, guild_id)).fetchone()

    def check_duel_giveup(self, userid, guild_id):
        query = f'''
            SELECT id, challenger, challengee, start_time, problem_name, contest_id, p_index, type FROM duel
            WHERE (challenger = ? OR challengee = ?) AND guild_id = ? AND status == {Duel.ONGOING}
        '''
        return self.conn.execute(query, (userid, userid, guild_id)).fetchone()


    def check_duel_complete(self, userid, guild_id):
        query = f'''
            SELECT id, challenger, challengee, start_time, problem_name, contest_id, p_index, type FROM duel
            WHERE (challenger = ? OR challengee = ?) AND guild_id = ? AND status == {Duel.ONGOING}
        '''
        return self.conn.execute(query, (userid, userid, guild_id)).fetchone()

    def create_duel(self, challenger, challengee, issue_time, prob, dtype, guild_id):
        query = f'''
            INSERT INTO duel (challenger, challengee, issue_time, problem_name, contest_id, p_index, status, type, guild_id) VALUES (?, ?, ?, ?, ?, ?, {Duel.PENDING}, ?, ?)
        '''
        duelid = self.conn.execute(query, (challenger, challengee, issue_time, prob.name, prob.contestId, prob.index, dtype, guild_id)).lastrowid
        self.conn.commit()
        return duelid

    def cancel_duel(self, duelid, guild_id, status):
        query = f'''
            UPDATE duel SET status = ? WHERE id = ? AND guild_id = ? AND status = {Duel.PENDING}
        '''
        rc = self.conn.execute(query, (status, duelid, guild_id)).rowcount
        if rc != 1:
            self.conn.rollback()
            return 0
        self.conn.commit()
        return rc

    def invalidate_duel(self, duelid, guild_id):
        query = f'''
            UPDATE duel SET status = {Duel.INVALID} WHERE id = ? AND guild_id = ? AND status = {Duel.ONGOING}
        '''
        rc = self.conn.execute(query, (duelid,guild_id)).rowcount
        if rc != 1:
            self.conn.rollback()
            return 0
        self.conn.commit()
        return rc

    def start_duel(self, duelid, guild_id, start_time):
        query = f'''
            UPDATE duel SET start_time = ?, status = {Duel.ONGOING}
            WHERE id = ? AND guild_id = ? AND status = {Duel.PENDING}
        '''
        rc = self.conn.execute(query, (start_time, duelid, guild_id)).rowcount
        if rc != 1:
            self.conn.rollback()
            return 0
        self.conn.commit()
        return rc

    def complete_duel(self, duelid, guild_id, winner, finish_time, winner_id = -1, loser_id = -1, delta = 0, dtype = DuelType.OFFICIAL):
        query = f'''
            UPDATE duel SET status = {Duel.COMPLETE}, finish_time = ?, winner = ? WHERE id = ? AND guild_id = ? AND status = {Duel.ONGOING}
        '''
        rc = self.conn.execute(query, (finish_time, winner, duelid, guild_id)).rowcount
        if rc != 1:
            self.conn.rollback()
            return 0

        if dtype == DuelType.OFFICIAL or dtype == DuelType.ADJOFFICIAL:
            self.update_duel_rating(winner_id, guild_id, +delta)
            self.update_duel_rating(loser_id, guild_id, -delta)

        self.conn.commit()
        return 1

    def update_duel_rating(self, userid, guild_id, delta):
        query = '''
            UPDATE duelist SET rating = rating + ? WHERE user_id = ? AND guild_id = ?
        '''
        rc = self.conn.execute(query, (delta, userid, guild_id)).rowcount
        self.conn.commit()
        return rc

    def get_duel_wins(self, userid, guild_id):
        query = f'''
            SELECT start_time, finish_time, problem_name, challenger, challengee FROM duel
            WHERE ((challenger = ? AND winner == {Winner.CHALLENGER}) OR (challengee = ? AND winner == {Winner.CHALLENGEE})) AND status = {Duel.COMPLETE} AND guild_id = ?
        '''
        return self.conn.execute(query, (userid, userid, guild_id)).fetchall()

    def get_duels(self, userid, guild_id):
        query = f'''
            SELECT id, start_time, finish_time, problem_name, challenger, challengee, winner FROM duel WHERE (challengee = ? OR challenger = ?) AND guild_id = ? AND status == {Duel.COMPLETE} ORDER BY start_time DESC
        '''
        return self.conn.execute(query, (userid, userid, guild_id)).fetchall()

    def get_duel_problem_names(self, userid, guild_id):
        query = f'''
            SELECT problem_name FROM duel WHERE (challengee = ? OR challenger = ?) AND guild_id = ? AND (status == {Duel.COMPLETE} OR status == {Duel.INVALID})
        '''
        return self.conn.execute(query, (userid, userid, guild_id)).fetchall()

    def get_pair_duels(self, userid1, userid2, guild_id):
        query = f'''
            SELECT id, start_time, finish_time, problem_name, challenger, challengee, winner FROM duel
            WHERE ((challenger = ? AND challengee = ?) OR (challenger = ? AND challengee = ?)) AND guild_id = ? AND status == {Duel.COMPLETE} ORDER BY start_time DESC
        '''
        return self.conn.execute(query, (userid1, userid2, userid2, userid1, guild_id)).fetchall()

    def get_recent_duels(self, guild_id):
        query = f'''
            SELECT id, start_time, finish_time, problem_name, challenger, challengee, winner FROM duel WHERE status == {Duel.COMPLETE} AND guild_id = ? ORDER BY start_time DESC LIMIT 7
        '''
        return self.conn.execute(query, (guild_id,)).fetchall()

    def get_ongoing_duels(self, guild_id):
        query = f'''
            SELECT id, challenger, challengee, start_time, problem_name, contest_id, p_index, type FROM duel
            WHERE status == {Duel.ONGOING} AND guild_id = ? ORDER BY start_time DESC
        '''
        return self.conn.execute(query, (guild_id,)).fetchall()

    def get_num_duel_completed(self, userid, guild_id):
        query = f'''
            SELECT COUNT(*) FROM duel WHERE (challengee = ? OR challenger = ?) AND guild_id = ? AND status == {Duel.COMPLETE}
        '''
        res = self.conn.execute(query, (userid, userid, guild_id)).fetchone()
        return res[0] if res else 0

    def get_num_duel_draws(self, userid, guild_id):
        query = f'''
            SELECT COUNT(*) FROM duel WHERE (challengee = ? OR challenger = ?) AND guild_id = ? AND winner == {Winner.DRAW}
        '''
        res = self.conn.execute(query, (userid, userid, guild_id)).fetchone()
        return res[0] if res else 0

    def get_num_duel_losses(self, userid, guild_id):
        query = f'''
            SELECT COUNT(*) FROM duel
            WHERE ((challengee = ? AND winner == {Winner.CHALLENGER}) OR (challenger = ? AND winner == {Winner.CHALLENGEE})) AND guild_id = ? AND status = {Duel.COMPLETE}
        '''
        res = self.conn.execute(query, (userid, userid, guild_id)).fetchone()
        return res[0] if res else 0

    def get_num_duel_declined(self, userid, guild_id):
        query = f'''
            SELECT COUNT(*) FROM duel WHERE challengee = ? AND guild_id = ? AND status == {Duel.DECLINED}
        '''
        res = self.conn.execute(query, (userid, guild_id)).fetchone()
        return res[0] if res else 0

    def get_num_duel_rdeclined(self, userid, guild_id):
        query = f'''
            SELECT COUNT(*) FROM duel WHERE challenger = ? AND guild_id = ? AND status == {Duel.DECLINED}
        '''
        res = self.conn.execute(query, (userid,guild_id)).fetchone()
        return res[0] if res else 0

    def get_duel_rating(self, userid, guild_id):
        query = '''
            SELECT rating FROM duelist WHERE user_id = ? AND guild_id = ?
        '''
        res = self.conn.execute(query, (userid,guild_id)).fetchone()
        return res[0] if res else 0

    def is_duelist(self, userid, guild_id):
        query = '''
            SELECT 1 FROM duelist WHERE user_id = ? AND guild_id = ?
        '''
        return self.conn.execute(query, (userid,guild_id)).fetchone()

    def register_duelist(self, userid, guild_id):
        query = '''
            INSERT OR IGNORE INTO duelist (user_id, rating, guild_id)
            VALUES (?, 1500, ?)
        '''
        with self.conn:
            return self.conn.execute(query, (userid,guild_id)).rowcount

    def get_duelists(self, guild_id):
        query = '''
            SELECT user_id, rating FROM duelist WHERE guild_id = ? ORDER BY rating DESC
        '''
        return self.conn.execute(query, (guild_id,)).fetchall()

    def get_complete_official_duels(self, guild_id):
        query = f'''
            SELECT challenger, challengee, winner, finish_time FROM duel WHERE status={Duel.COMPLETE}
            AND (type={DuelType.OFFICIAL} OR type={DuelType.ADJOFFICIAL}) AND guild_id = ? ORDER BY finish_time ASC
        '''
        return self.conn.execute(query, (guild_id,)).fetchall()

    def get_rankup_channel(self, guild_id):
        query = ('SELECT channel_id '
                 'FROM rankup '
                 'WHERE guild_id = ?')
        channel_id = self.conn.execute(query, (guild_id,)).fetchone()
        return int(channel_id[0]) if channel_id else None

    def set_rankup_channel(self, guild_id, channel_id):
        query = ('INSERT OR REPLACE INTO rankup '
                 '(guild_id, channel_id) '
                 'VALUES (?, ?)')
        with self.conn:
            self.conn.execute(query, (guild_id, channel_id))

    def clear_rankup_channel(self, guild_id):
        query = ('DELETE FROM rankup '
                 'WHERE guild_id = ?')
        with self.conn:
            return self.conn.execute(query, (guild_id,)).rowcount

    def enable_auto_role_update(self, guild_id):
        query = ('INSERT OR REPLACE INTO auto_role_update '
                 '(guild_id) '
                 'VALUES (?)')
        with self.conn:
            return self.conn.execute(query, (guild_id,)).rowcount

    def disable_auto_role_update(self, guild_id):
        query = ('DELETE FROM auto_role_update '
                 'WHERE guild_id = ?')
        with self.conn:
            return self.conn.execute(query, (guild_id,)).rowcount

    def has_auto_role_update_enabled(self, guild_id):
        query = ('SELECT 1 '
                 'FROM auto_role_update '
                 'WHERE guild_id = ?')
        return self.conn.execute(query, (guild_id,)).fetchone() is not None

    def reset_status(self, id):
        inactive_query = '''
            UPDATE user_handle
            SET active = 0
            WHERE guild_id = ?
        '''
        self.conn.execute(inactive_query, (id,))
        self.conn.commit()

    def update_status(self, guild_id: str, active_ids: list):
        placeholders = ', '.join(['?'] * len(active_ids))
        if not active_ids: return 0
        active_query = '''
            UPDATE user_handle
            SET active = 1
            WHERE user_id IN ({})
            AND guild_id = ?
        '''.format(placeholders)
        rc = self.conn.execute(active_query, (*active_ids, guild_id)).rowcount
        self.conn.commit()
        return rc

    # Rated VC stuff

    def create_rated_vc(self, contest_id: int, start_time: float, finish_time: float, guild_id: str, user_ids: list[str]):
        """ Creates a rated vc and returns its id.
        """
        query = ('INSERT INTO rated_vcs '
                 '(contest_id, start_time, finish_time, status, guild_id) '
                 'VALUES ( ?, ?, ?, ?, ?)')
        id = None
        with self.conn:
            id = self.conn.execute(query, (contest_id, start_time, finish_time, RatedVC.ONGOING, guild_id)).lastrowid
            for user_id in user_ids:
                query = ('INSERT INTO rated_vc_users '
                         '(vc_id, user_id) '
                         'VALUES (? , ?)')
                self.conn.execute(query, (id, user_id))
        return id

    def get_rated_vc(self, vc_id: int):
        query = ('SELECT * '
                'FROM rated_vcs '
                'WHERE id = ? ')
        vc = self._fetchone(query, params=(vc_id,), row_factory=namedtuple_factory)
        return vc

    def get_ongoing_rated_vc_ids(self):
        query = ('SELECT id '
                 'FROM rated_vcs '
                 'WHERE status = ? '
                 )
        vcs = self._fetchall(query, params=(RatedVC.ONGOING,), row_factory=namedtuple_factory)
        vc_ids = [vc.id for vc in vcs]
        return vc_ids

    def get_rated_vc_user_ids(self, vc_id: int):
        query = ('SELECT user_id '
                 'FROM rated_vc_users '
                 'WHERE vc_id = ? '
                 )
        users = self._fetchall(query, params=(vc_id,), row_factory=namedtuple_factory)
        user_ids = [user.user_id for user in users]
        return user_ids

    def finish_rated_vc(self, vc_id: int):
        query = ('UPDATE rated_vcs '
                'SET status = ? '
                'WHERE id = ? ')

        with self.conn:
            self.conn.execute(query, (RatedVC.FINISHED, vc_id))

    def update_vc_rating(self, vc_id: int, user_id: str, rating: int):
        query = ('INSERT OR REPLACE INTO rated_vc_users '
                 '(vc_id, user_id, rating) '
                 'VALUES (?, ?, ?) ')

        with self.conn:
            self.conn.execute(query, (vc_id, user_id, rating))

    def get_vc_rating(self, user_id: str, default_if_not_exist: bool = True):
        query = ('SELECT MAX(vc_id) AS latest_vc_id, rating '
                 'FROM rated_vc_users '
                 'WHERE user_id = ? AND rating IS NOT NULL'
                 )
        rating = self._fetchone(query, params=(user_id, ), row_factory=namedtuple_factory).rating
        if rating is None:
            if default_if_not_exist:
                return _DEFAULT_VC_RATING
            return None
        return rating

    def get_vc_rating_history(self, user_id: str):
        """ Return [vc_id, rating].
        """
        query = ('SELECT vc_id, rating '
                 'FROM rated_vc_users '
                 'WHERE user_id = ? AND rating IS NOT NULL'
                 )
        ratings = self._fetchall(query, params=(user_id,), row_factory=namedtuple_factory)
        return ratings

    def set_rated_vc_channel(self, guild_id, channel_id):
        query = ('INSERT OR REPLACE INTO rated_vc_settings '
                 ' (guild_id, channel_id) VALUES (?, ?)'
                 )
        with self.conn:
            self.conn.execute(query, (guild_id, channel_id))

    def get_rated_vc_channel(self, guild_id):
        query = ('SELECT channel_id '
                 'FROM rated_vc_settings '
                 'WHERE guild_id = ?')
        channel_id = self.conn.execute(query, (guild_id,)).fetchone()
        return int(channel_id[0]) if channel_id else None

    def remove_last_ratedvc_participation(self, user_id: str):
        query = ('SELECT MAX(vc_id) AS vc_id '
                 'FROM rated_vc_users '
                 'WHERE user_id = ? '
                 )
        vc_id = self._fetchone(query, params=(user_id, ), row_factory=namedtuple_factory).vc_id
        query = ('DELETE FROM rated_vc_users '
                 'WHERE user_id = ? AND vc_id = ? ')
        with self.conn:
            return self.conn.execute(query, (user_id, vc_id)).rowcount

    def set_training_channel(self, guild_id, channel_id):
        query = ('INSERT OR REPLACE INTO training_settings '
                 ' (guild_id, channel_id) VALUES (?, ?)'
                 )
        with self.conn:
            self.conn.execute(query, (guild_id, channel_id))

    def get_training_channel(self, guild_id):
        query = ('SELECT channel_id '
                 'FROM training_settings '
                 'WHERE guild_id = ?')
        channel_id = self.conn.execute(query, (guild_id,)).fetchone()
        return int(channel_id[0]) if channel_id else None

    def new_training(self, user_id, issue_time, prob, mode, score, lives, time_left):
        query1 = f'''
            INSERT INTO trainings
            (user_id, score, lives, time_left, mode, status)
            VALUES
            (?, 0, ?, ?, ?, {Training.ACTIVE})
        '''
        query2 = f'''
            INSERT INTO training_problems (training_id, issue_time, problem_name, contest_id, p_index, rating, status)
            VALUES (?, ?, ?, ?, ?, ?, {TrainingProblemStatus.ACTIVE})
        '''
        cur = self.conn.cursor()
        cur.execute(query1, (user_id, lives, time_left, mode))
        training_id, rc = cur.lastrowid, cur.rowcount
        if rc != 1:
            self.conn.rollback()
            return 0
        cur.execute(query2, (training_id, issue_time, prob.name, prob.contestId, prob.index, prob.rating))
        if cur.rowcount != 1:
            self.conn.rollback()
            return 0
        self.conn.commit()
        return 1


    def get_active_training(self, user_id):
        query1 = f'''
            SELECT id, mode, score, lives, time_left FROM trainings
            WHERE user_id = ? AND status = {Training.ACTIVE}
        '''
        res = self.conn.execute(query1, (user_id,)).fetchone()
        if res is None: return None
        training_id,mode,score,lives,time_left = res
        query2 = f'''
            SELECT issue_time, problem_name, contest_id, p_index, rating FROM training_problems
            WHERE training_id = ? AND status = {TrainingProblemStatus.ACTIVE}
        '''
        res = self.conn.execute(query2, (training_id,)).fetchone()
        if res is None: return None
        return training_id, res[0], res[1], res[2], res[3], res[4], mode, score, lives,time_left

    def get_latest_training(self, user_id):
        query1 = f'''
            SELECT id, mode, score, lives, time_left FROM trainings
            WHERE user_id = ? AND status = {Training.COMPLETED} ORDER BY id DESC
        '''
        res = self.conn.execute(query1, (user_id,)).fetchone()
        if res is None: return None
        training_id,mode,score,lives,time_left = res
        return training_id, None, None, None, None, None, mode, score, lives,time_left


    def end_current_training_problem(self, training_id, finish_time, status, score, lives, time_left):
        query1 = f'''
            UPDATE training_problems SET finish_time = ?, status = ?
            WHERE training_id = ? AND status = {TrainingProblemStatus.ACTIVE}
        '''
        query2 = '''
            UPDATE trainings SET score = ?, lives = ?, time_left = ?
            WHERE id = ?
        '''
        rc = self.conn.execute(query1, (finish_time, status, training_id)).rowcount
        if rc != 1:
            self.conn.rollback()
            return -1
        rc = self.conn.execute(query2, (score, lives, time_left, training_id)).rowcount
        if rc != 1:
            self.conn.rollback()
            return -2
        self.conn.commit()
        return 1

    def assign_training_problem(self, training_id, issue_time, prob):
        query1 = f'''
            INSERT INTO training_problems (training_id, issue_time, problem_name, contest_id, p_index, rating, status)
            VALUES (?, ?, ?, ?, ?, ?, {TrainingProblemStatus.ACTIVE})
        '''

        cur = self.conn.cursor()
        cur.execute(query1, (training_id, issue_time, prob.name, prob.contestId, prob.index, prob.rating))
        if cur.rowcount != 1:
            self.conn.rollback()
            return -1
        self.conn.commit()
        return 1

    def finish_training(self, training_id):
        query1 = f'''
            UPDATE trainings SET status = {Training.COMPLETED}
            WHERE id = ?
        '''
        rc = self.conn.execute(query1, (training_id,)).rowcount
        if rc != 1:
            self.conn.rollback()
            return -1
        self.conn.commit()
        return 1

    def get_training_skips(self, user_id):
        query = f'''
            SELECT tp.problem_name
            FROM training_problems tp, trainings tr
            WHERE tp.training_id = tr.id
            AND (tp.status = {TrainingProblemStatus.SKIPPED} OR tp.status = {TrainingProblemStatus.INVALIDATED})
            AND tr.user_id = ?
        '''
        return {name for name, in self.conn.execute(query, (user_id,)).fetchall()}


    def train_get_num_solves(self, training_id):
        query = f'''
            SELECT COUNT(*) FROM training_problems
            WHERE training_id = ? AND status == {TrainingProblemStatus.SOLVED}
        '''
        return self.conn.execute(query, (training_id,)).fetchone()[0]

    def train_get_num_skips(self, training_id):
        query = f'''
            SELECT COUNT(*) FROM training_problems
            WHERE training_id = ? AND status == {TrainingProblemStatus.SKIPPED}
        '''
        return self.conn.execute(query, (training_id,)).fetchone()[0]

    def train_get_num_slow_solves(self, training_id):
        query = f'''
            SELECT COUNT(*) FROM training_problems
            WHERE training_id = ? AND status == {TrainingProblemStatus.SOLVED_TOO_SLOW}
        '''
        return self.conn.execute(query, (training_id,)).fetchone()[0]

    def train_get_start_rating(self, training_id):
        query = f'''
            SELECT rating FROM training_problems
            WHERE training_id = ?
        '''
        return self.conn.execute(query, (training_id,)).fetchone()[0]

    def train_get_max_rating(self, training_id):
        query = f'''
            SELECT MAX(rating) FROM training_problems
            WHERE training_id = ? AND status == {TrainingProblemStatus.SOLVED}
        '''
        return self.conn.execute(query, (training_id,)).fetchone()[0]

    def train_get_fastest_solves(self):
        query = f'''
            SELECT tr.user_id, tp.rating, min(tp.finish_time-tp.issue_time)
            FROM training_problems tp, trainings tr
            WHERE tp.training_id = tr.id
            AND (tp.status = {TrainingProblemStatus.SOLVED} OR tp.status = {TrainingProblemStatus.SOLVED_TOO_SLOW})
            GROUP BY tp.rating
        '''
        return self.conn.execute(query).fetchall()

    ### Lockout round


    def set_round_channel(self, guild_id, channel_id):
        query = ('INSERT OR REPLACE INTO round_settings '
                 ' (guild_id, channel_id) VALUES (?, ?)'
                 )
        with self.conn:
            self.conn.execute(query, (guild_id, channel_id))

    def get_round_channel(self, guild_id):
        query = ('SELECT channel_id '
                 'FROM round_settings '
                 'WHERE guild_id = ?')
        channel_id = self.conn.execute(query, (guild_id,)).fetchone()
        return int(channel_id[0]) if channel_id else None

    def create_ongoing_round(self, guild_id, timestamp, users, rating, points, problems, duration, repeat):
        query = f'''
            INSERT INTO lockout_ongoing_rounds (guild, users, rating, points, time, problems, status, duration, repeat, times)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        cur = self.conn.cursor()
        cur.execute(query, (guild_id, ' '.join([f"{x.id}" for x in users]), 
                                      ' '.join(map(str, rating)),
                                      ' '.join(map(str, points)), 
                                      timestamp, 
                                      ' '.join([f"{x.contestId}/{x.index}" for x in problems]), 
                                      ' '.join('0' for i in range(len(users))),
                                      duration, 
                                      repeat, 
                                      ' '.join(['0'] * len(users)))
                    )
        self.conn.commit()
        cur.close()

    def create_finished_round(self, round_info, timestamp):
        query = f'''
                    INSERT INTO lockout_finished_rounds (guild, users, rating, points, time, problems, status, duration, repeat, times, end_time)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                '''
        cur = self.conn.cursor()
        cur.execute(query, (round_info.guild, round_info.users, round_info.rating, round_info.points, round_info.time,
                                round_info.problems, round_info.status, round_info.duration, round_info.repeat,
                                round_info.times, timestamp))
        self.conn.commit()
        cur.close()                

    def update_round_status(self, guild, user, status, problems, timestamp):
        query = f"""
                    UPDATE lockout_ongoing_rounds 
                    SET
                    status = ?, 
                    problems = ?,
                    times = ?
                    WHERE
                    guild = ? AND users LIKE ? 
                """
        cur = self.conn.cursor()
        cur.execute(query,
                     (' '.join([str(x) for x in status]), ' '.join(problems), ' '.join([str(x) for x in timestamp]),
                      guild, f"%{user}%"))
        self.conn.commit()
        cur.close()

    def get_round_info(self, guild_id, users):
        query = f'''
                    SELECT * FROM lockout_ongoing_rounds
                    WHERE
                    guild = ? AND users LIKE ?
                 '''
        cur = self.conn.cursor()
        cur.execute(query, (guild_id, f"%{users}%"))
        data = cur.fetchone()
        cur.close()
        Round = namedtuple('Round', 'guild users rating points time problems status duration repeat times')
        return Round(data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8], data[9], data[10])

    def check_if_user_in_ongoing_round(self, guild, user):
        query = f'''
                    SELECT * FROM lockout_ongoing_rounds
                    WHERE
                    users LIKE ? AND guild = ?
                '''
        cur = self.conn.cursor()
        cur.execute(query, (f"%{user}%", guild))
        data = cur.fetchall()
        cur.close()
        if len(data) > 0:
            return True
        return False

    def delete_round(self, guild, user):
        query = f'''
                    DELETE FROM lockout_ongoing_rounds
                    WHERE
                    guild = ? AND users LIKE ?
                '''
        cur = self.conn.cursor()
        cur.execute(query, (guild, f"%{user}%"))
        self.conn.commit()
        cur.close()    

    def get_ongoing_rounds(self, guild):
        query = f'''
                    SELECT * FROM lockout_ongoing_rounds WHERE guild = ?
                '''
        cur = self.conn.cursor()
        cur.execute(query, (guild,))
        res = cur.fetchall()
        cur.close()
        Round = namedtuple('Round', 'guild users rating points time problems status duration repeat times')
        return [Round(data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8], data[9], data[10]) for data in res]

    def get_recent_rounds(self, guild, user=None):
        query = f'''
                    SELECT * FROM lockout_finished_rounds 
                    WHERE guild = ? AND users LIKE ?
                    ORDER BY end_time DESC
                '''
        cur = self.conn.cursor()
        cur.execute(query, (guild, '%' if user is None else f'%{user}%'))
        res = cur.fetchall()
        cur.close()
        Round = namedtuple('Round', 'guild users rating points time problems status duration repeat times end_time')
        return [Round(data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8], data[9], data[10], data[11]) for data in res]

    def add_role_reaction(self, message_id: int, role_id: int, emoji: str):
        query = '''
            INSERT INTO role_reactions (message_id, role_id, emoji)
            VALUES (?, ?, ?)
        '''
        with self.conn:
            self.conn.execute(query, (message_id, role_id, emoji))
        self.role_cache[(message_id, emoji)] = role_id

    def remove_role_reaction(self, message_id: int, emoji: str):
        query = '''
            DELETE FROM role_reactions
            WHERE message_id = ? AND emoji = ?
        '''
        with self.conn:
            self.conn.execute(query, (message_id, emoji))
        self.role_cache.pop((message_id, emoji), None)

    def get_role_reaction(self, message_id: int, emoji: str):
        return self.role_cache.get((message_id, emoji))
    
    def is_gym_member(self, member_id: int):
        query = '''
            SELECT COUNT(discord_id)
            FROM gym_members
            WHERE discord_id = ?
        '''
        return self.conn.execute(query, (member_id,)).fetchone()[0] == 1
        
    def create_gym_member(self, member_id: int, timezone: str, units: bool):
        query = '''
            INSERT INTO gym_members (discord_id, units, tz)
            VALUES (?, ?, ?)
        '''
        self.conn.execute(query, (member_id, units, timezone))
    
    def get_gym_member(self, member_id: int, fields: list[str]):
        query = '''
            SELECT '''+', '.join(fields)+''' FROM gym_members
            WHERE discord_id = ?
        '''
        return self.conn.execute(query, (member_id,)).fetchone()

    def update_gym_member(self, member_id: int, fields: dict[str, str]):
        if 'tz' in fields:
            query = '''
                SELECT id, datetime FROM gym_sessions
                WHERE status = "incomplete" AND user = ?
            '''
            lst = self.conn.execute(query, (member_id,)).fetchall()
            lst2 = []
            for i in lst:
                lst2+=[int(datetime.datetime.fromtimestamp(i[1],tz=zoneinfo.ZoneInfo(fields['tz'][0])).replace(tzinfo=zoneinfo.ZoneInfo(fields['tz'][1])).timestamp())]
            for j in paginator.chunkify(list(zip(lst, lst2)), 500):
                query = '''
                    UPDATE gym_sessions
                    SET datetime = CASE id
                '''+('WHEN ? THEN ?' * len(j))+'''
                    ELSE datetime
                    END
                    WHERE id IN ('''+", ".join(["?"]*len(j))+''')
                '''
                self.conn.execute(query, [x for i in j for x in (i[0][0], i[1])]+[i[0][0] for i in j])
            query = '''
                SELECT id, next FROM gym_recurring_sessions
                WHERE next > ? AND user = ?
            '''
            lst = self.conn.execute(query, (int(datetime.datetime.now().timestamp()), member_id)).fetchall()
            lst2 = []
            for i in lst:
                lst2+=[int(datetime.datetime.fromtimestamp(i[1],tz=zoneinfo.ZoneInfo(fields['tz'][0])).replace(tzinfo=zoneinfo.ZoneInfo(fields['tz'][1])).timestamp())]
            for j in paginator.chunkify(list(zip(lst, lst2)), 500):
                query = '''
                    UPDATE gym_recurring_sessions
                    SET next = CASE id
                '''+('WHEN ? THEN ?' * len(j))+'''
                    ELSE next
                    END
                    WHERE id IN ('''+", ".join(["?"]*len(j))+''')
                '''
                self.conn.execute(query, [x for i in j for x in (i[0][0], i[1])]+[i[0][0] for i in j])
        query = '''
            UPDATE gym_members
            SET '''+', '.join([i+" = ?" for i in fields])+'''
            WHERE discord_id = ?
        '''
        self.conn.execute(query, [i[1] for i in fields.values()]+[member_id])
    def create_session(self, member_id: int, datetime: int):
        query = '''
            INSERT INTO gym_sessions (user, datetime, status)
            VALUES (?, ?, ?)
        '''
        self.conn.execute(query, (member_id, datetime, "unresponded"))
    def get_sessions(self, member_id: int, fields: list[str]):
        query = '''
            SELECT '''+', '.join(fields)+''' FROM gym_sessions
            WHERE user = ?
            ORDER BY datetime DESC
        '''
        return self.conn.execute(query, (member_id,))
    def skip_days(self, timestamp : int, n : int, tz : str):
    
        dt = datetime.datetime.fromtimestamp(timestamp, tz=zoneinfo.ZoneInfo(tz)) + datetime.timedelta(days=7*n)
        return int(dt.timestamp())
    
    def skip_session(self, member_id: int, datetime: int, reason: str, tz:str, check_recurring: bool = True):
        query = '''
            UPDATE gym_sessions
            SET status = ?
            WHERE user = ? AND datetime = ? AND status = "unresponded"
        '''
        if self.conn.execute(query, ("skipped|"+reason, member_id, datetime)).rowcount == 0:
            return False
        if not check_recurring:
            return True
        
        query = '''
            SELECT id FROM gym_recurring_sessions
            WHERE next = ? AND user = ?
        '''
        value = self.conn.execute(query, (datetime, member_id)).fetchone()
        if value:
            self.create_session(member_id, self.skip_days(datetime, 1, tz))
            query = '''
                UPDATE gym_recurring_sessions
                SET next = ?
                WHERE next = ? AND user = ?
            '''
            self.conn.execute(query, (self.skip_days(datetime, 1, tz), datetime, member_id))
        return True
    def start_session(self, member_id: int):
        dtnow = int(datetime.datetime.now().timestamp())
        query = '''
            SELECT datetime FROM gym_sessions
            WHERE user = ? AND status = "inprogress"
        '''
        dt = self.conn.execute(query, (member_id,)).fetchone()
        if dt:
            return False
        query = '''
            SELECT datetime FROM gym_sessions
            WHERE datetime < ? AND datetime > ? AND user = ? AND status = "unresponded"
            ORDER BY datetime ASC
        '''
        
        dt = self.conn.execute(query, (dtnow+3600, dtnow-3600, member_id)).fetchone()
        if not dt:
            self.create_session(member_id, dtnow)
            dt = self.conn.execute(query, (dtnow+3600, dtnow-3600, member_id)).fetchone()

        query = '''
            UPDATE gym_sessions
            SET status = "inprogress"
            WHERE user = ? AND datetime = ? AND status = "unresponded"
        '''
        self.conn.execute(query, (member_id, dt[0]))
        return True
    def end_session(self, member_id: int, tz: str):
        query = '''
            SELECT datetime FROM gym_sessions
            WHERE user = ? AND status = "inprogress"
        '''
        dt = self.conn.execute(query, (member_id,)).fetchone()
        if not dt:
            return False
        query = '''
            UPDATE gym_sessions
            SET status = "complete"
            WHERE user = ? AND status = "inprogress"
        '''
        self.conn.execute(query, (member_id,))
        
        query = '''
            SELECT next FROM gym_recurring_sessions
            WHERE next = ? AND user = ?
        '''
        if self.conn.execute(query, (dt[0], member_id)).fetchone():
            self.create_session(member_id, self.skip_days(dt[0], 1, tz))
            query = '''
                UPDATE gym_recurring_sessions
                SET next = ?
                WHERE next = ? AND user = ?
            '''
            self.conn.execute(query, (self.skip_days(dt[0], 1, tz), dt[0], member_id))
        return True
    def get_session(self, id: int, member_id: int, fields: list[str]):
        query = '''
            SELECT '''+', '.join(fields)+''' FROM gym_sessions
            WHERE id = ? AND user = ?
        '''
        return self.conn.execute(query, (id, member_id)).fetchone()
    
    def get_workouts(self, session_id: int, member_id: int, fields: list[str]):
        query = '''
            SELECT '''+', '.join(fields)+''' FROM gym_workouts
            WHERE session = ? AND user = ?
        '''
        return self.conn.execute(query, (session_id, member_id))
    def get_records(self, fields: list[str]):
        query = ('SELECT '+', '.join(fields)+' FROM gym_records')
        return self.conn.execute(query)
    def get_records_for_exercise(self, exercise: str):
        query = '''
            SELECT gym_records.type, gym_records.amount, gym_workouts.user, gym_sessions.datetime FROM gym_records
            INNER JOIN gym_workouts ON gym_workouts.id = gym_records.workout AND gym_workouts.exercise = ?
            INNER JOIN gym_sessions ON gym_workouts.session = gym_sessions.id
        '''
        return self.conn.execute(query, (exercise,))
    def update_record(self, exercise: str):
        for i in ["sets", "reps", "time", "weight", "length"]:
            query = '''
                INSERT INTO gym_records (exercise, type, amount, workout)
                SELECT ?, ?, gym_workouts.'''+i+''', gym_workouts.id FROM gym_workouts
                ORDER BY '''+i+''' DESC
                LIMIT 1
                ON CONFLICT(exercise, type)
                DO UPDATE SET amount = excluded.amount, workout = excluded.workout;
            '''
            try:
                self.conn.execute(query, (exercise, i))
            except:
                pass
    def add_workout(self, member_id: int, session_id: int, exercise_name: str, sets: int, reps: int, time: float|None, weight: float|None, length: float|None):
        query = '''
            INSERT INTO gym_workouts (user, session, exercise, time, weight, length, sets, reps)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        '''
        self.conn.execute(query, (member_id, session_id, exercise_name, time, weight, length, sets, reps))
        self.update_record(exercise_name)
    
    def remove_workout(self, id: int, member_id: int):
        query = '''
            SELECT exercise FROM gym_workouts
            WHERE id = ? AND user = ?
        '''
        exercise = self.conn.execute(query, (id, member_id)).fetchone()
        if not exercise:
            return False
        query = '''
            DELETE FROM gym_sessions
            WHERE id = ?
        '''
        self.conn.execute(query, (id,))
        self.update_record(exercise[0])
        return True
    def daytime_to_datetime(self, day : int, time : int, n : int, tz : str):
        now = datetime.datetime.now(tz=zoneinfo.ZoneInfo(tz))
        dt = now.date() + datetime.timedelta(days=(day - now.weekday())%7 + 7*n)
        dt = (datetime.datetime.combine(dt, datetime.datetime.fromtimestamp(time, datetime.UTC).time(), tzinfo=zoneinfo.ZoneInfo(tz)))
        if dt.timestamp() < datetime.datetime.now().timestamp():
            dt+=datetime.timedelta(days=7)
        return int(dt.timestamp())
    
    def create_recurring_session(self, member_id: int, day : int, time : int, tz : str):
        self.create_session(member_id, self.daytime_to_datetime(day, time, 0, tz))
        query = '''
            INSERT INTO gym_recurring_sessions (user, day, time, next)
            VALUES (?, ?, ?, ?)
        '''
        self.conn.execute(query, (member_id, day, time, self.daytime_to_datetime(day, time, 0, tz)))
    
    def get_recurring_sessions(self, member_id: int, fields : list[str]):
        query = '''
            SELECT '''+', '.join(fields)+''' FROM gym_recurring_sessions
            WHERE user = ?
        '''
        return self.conn.execute(query, (member_id,))
    
    def get_recurring_sessions_by_day(self, member_id: int, day: int, fields : list[str]):
        query = '''
            SELECT '''+', '.join(fields)+''' FROM gym_recurring_sessions
            WHERE user = ? AND day = ?
        '''
        return self.conn.execute(query, (member_id, day))
    
    def remove_recurring_session(self, member_id: int, day : int, time : int):
        query = '''
            DELETE FROM gym_recurring_sessions
            WHERE user = ? AND day = ? AND time = ?
        '''
        return self.conn.execute(query, (member_id, day, time)).rowcount != 0
    
    def skip_recurring_session(self, member_id: int, day : int, time : int, n: int, reason: str, tz : str):
        query = '''
            SELECT next FROM gym_recurring_sessions
            WHERE day = ? AND time = ? AND user = ?
        '''
        value = self.conn.execute(query, (day, time, member_id)).fetchone()
        if not value:
            return False
        self.create_session(member_id, self.skip_days(value[0], n, tz))
        query = '''
            UPDATE gym_recurring_sessions
            SET next = ?
            WHERE next = ? AND user = ?
        '''
        self.conn.execute(query, (self.skip_days(value[0], n, tz), value[0], member_id))
        return self.skip_session(member_id, value[0], reason, tz, False)
        
    def get_exercises(self):
        query = ('SELECT * FROM gym_exercises')
        return self.conn.execute(query)
    
    def create_exercise(self, name: str):
        query = '''
            INSERT INTO gym_exercises (name)
            VALUES (?)
        '''
        self.conn.execute(query, (name,))

    def update_exercise(self, name: str, new_name: str):
        query = '''
            UPDATE gym_exercises
            SET name = ?
            WHERE name = ?
        '''
        self.conn.execute(query, (new_name, name))
    
    def setup_guild(self, guild_id: int, channel_id: int, role_id: int):
        self.guild_cache[guild_id] = (channel_id, role_id)
        query = '''
            INSERT OR REPLACE INTO gym_guilds (guild, channel, role)
            VALUES (?, ?, ?)
        '''
        self.conn.execute(query, (guild_id, channel_id, role_id))

    def get_guild(self, guild_id: int):
        if guild_id in self.guild_cache:
            return self.guild_cache[guild_id]
        query = '''
            SELECT channel, role FROM gym_guilds
            WHERE guild = ?
        '''
        val = self.conn.execute(query, (guild_id,)).fetchone()
        self.guild_cache[guild_id] = val
        return val

    def get_incomplete_sessions(self):
        query = '''
            SELECT user FROM gym_sessions
            WHERE status = "unresponded" AND datetime < ?
        '''
        vals = self.conn.execute(query, (int(datetime.datetime.now().timestamp()-3600),)).fetchall()
        query = '''
            UPDATE gym_sessions
            SET status = "skipped|Did not start on time"
            WHERE status = "unresponded" AND datetime < ?
        '''
        self.conn.execute(query, (int(datetime.datetime.now().timestamp()-3600),))
        return vals
    def get_close_sessions(self):
        query = '''
            SELECT user, datetime FROM gym_sessions
            WHERE status = "unresponded" AND datetime < ? AND datetime > ?
        '''
        now = int(datetime.datetime.now().timestamp())
        return self.conn.execute(query, (now+1830, now+1770,)).fetchall()

    def get_open_sessions(self):
        query = '''
            SELECT user FROM gym_sessions
            WHERE status = "inprogress" AND datetime < ?
        '''
        vals = self.conn.execute(query, (int(datetime.datetime.now().timestamp()-259200),)).fetchall()
        query = '''
            UPDATE gym_sessions
            SET status = "complete"
            WHERE status = "inprogress" AND datetime < ?
        '''
        self.conn.execute(query, (int(datetime.datetime.now().timestamp()-259200),))
        return vals
    def fix_recurring_sessions(self):
        query = '''
            SELECT gym_recurring_sessions.id, gym_recurring_sessions.user, gym_recurring_sessions.next, gym_members.tz FROM gym_recurring_sessions
            INNER JOIN gym_members 
            ON gym_members.discord_id = gym_recurring_sessions.user AND gym_recurring_sessions.next < ?
        '''
        vals = self.conn.execute(query, (int(datetime.datetime.now().timestamp()),)).fetchall()
        for i in vals:
            self.create_session(vals[1], self.skip_days(vals[2], 1, vals[3]))
        for j in paginator.chunkify(vals, 500):
            query = '''
                UPDATE gym_recurring_sessions
                SET next = CASE id
            '''+('WHEN ? THEN ?' * len(j))++'''
                ELSE next
                END
                WHERE id IN ('''+", ".join(["?"]*len(j))+''')
            '''
            self.conn.execute(query, [x for i in j for x in (i[0], self.skip_days(i[2], 1, i[3]))] + [i[0] for i in j])
    def close(self):
        self.conn.close()

