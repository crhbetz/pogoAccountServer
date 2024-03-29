import argparse
import configparser
import logging
import time
from loguru import logger


config = configparser.ConfigParser()
config.read("config.ini")

parser = argparse.ArgumentParser(description='Pokemon GO PTC Account Server')
parser.add_argument('-v', '--verbose', action='store_true')
parser.add_argument('-vv', '--trace', action='store_true')


class Config:
    general = config["general"]
    listen_host = general.get("listen_host", "127.0.0.1")
    listen_port = general.getint("listen_port", 9009)
    auth_username = general.get("auth_username", None)
    auth_password = general.get("auth_password", None)
    cooldown_hours = general.getint("cooldown", 24)
    cooldown_seconds = cooldown_hours * 60 * 60
    rate_limit_minutes = general.getint("rate_limit_minutes", 60)
    rate_limit_number = general.getint("rate_limit_number", 3)
    strict_rate_limit_minutes = general.getint("strict_rate_limit_minutes", 5)
    strict_rate_limit_seconds = strict_rate_limit_minutes * 60
    allow_rate_limit_override_when_burned = general.getboolean("allow_rate_limit_override_when_burned", True)
    force_release_seconds = general.getint("force_release_days", 30) * 60 * 60 * 24

    args = parser.parse_args()
    if args.verbose:
        loglevel = logging.DEBUG
    elif args.trace:
        loglevel = logging.TRACE
    else:
        loglevel = logging.INFO

    database = config["database"]
    db_host = database.get("host", "127.0.0.1")
    db_port = database.getint("port", 3306)
    db_user = database.get("user", None)
    db_pw = database.get("pass", None)
    db = database.get("db", None)

    def __init__(self):
        if self.db_user is None or self.db_pw is None or self.db is None or self.auth_username is None \
                or self.auth_password is None:
            logger.error("Missing required setting! Check your config.")

    @classmethod
    def get_cooldown_timestamp(cls):
        res = int(int(time.time()) - cls.cooldown_seconds)
        logger.trace(f"calculated cooldown timestamp {res}")
        return res
