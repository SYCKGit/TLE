import os

DATA_DIR = 'data'
LOGS_DIR = 'logs'

ASSETS_DIR = os.path.join(DATA_DIR, 'assets')
DB_DIR = os.path.join(DATA_DIR, 'db')
MISC_DIR = os.path.join(DATA_DIR, 'misc')
TEMP_DIR = os.path.join(DATA_DIR, 'temp')

USER_DB_FILE_PATH = os.path.join(DB_DIR, 'user.db')
CACHE_DB_FILE_PATH = os.path.join(DB_DIR, 'cache.db')

FONTS_DIR = os.path.join(ASSETS_DIR, 'fonts')

NOTO_SANS_CJK_BOLD_FONT_PATH = os.path.join(FONTS_DIR, 'NotoSansCJK-Bold.ttc')
NOTO_SANS_CJK_REGULAR_FONT_PATH = os.path.join(FONTS_DIR, 'NotoSansCJK-Regular.ttc')

CONTEST_WRITERS_JSON_FILE_PATH = os.path.join(MISC_DIR, 'contest_writers.json')

LOG_FILE_PATH = os.path.join(LOGS_DIR, 'tle.log')

ALL_DIRS = (attrib_value for attrib_name, attrib_value in list(globals().items())
            if attrib_name.endswith('DIR'))

TLE_ADMIN = os.environ.get('TLE_ADMIN', 'Admin')
TLE_MODERATOR = os.environ.get('TLE_MODERATOR', 'Moderator')

SPOI_GUILD_ID = int(os.environ.get("SPOI_GUILD_ID", "0"))
VERIFIED_ROLE_ID = int(os.environ.get("VERIFIED_ROLE_ID", "0"))
ALUMNI_ROLE_ID = int(os.environ.get("ALUMNI_ROLE_ID", "0"))
UNVERIFIED_CHANNEL_ID = int(os.environ.get("UNVERIFIED_CHANNEL_ID", "0"))
DECISION_CHANNEL_ID = int(os.environ.get("DECISION_CHANNEL_ID", "0"))
