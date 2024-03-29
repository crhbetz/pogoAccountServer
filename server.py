import collections
import humanize
import logging
import os
import sys
import time

from enum import IntEnum
from flask import Flask, request
from flask_basicauth import BasicAuth
from loguru import logger
from operator import itemgetter

from config import Config
from db_connection import DbConnection as Db
from logs import setup_logger
from request_log import RequestLog
from utils import can_be_type


setup_logger()


class RateLimit(IntEnum):
    unlimited = 0
    burst = 1
    period = 2
    unknown = 3


class AccountServer:

    def __init__(self):
        logger.info("initializing server")
        self.config = Config()
        self.host = self.config.listen_host
        self.port = self.config.listen_port
        self.resp_headers = {"Server": "pogoAccountServer"
                             }
        self.request_log = RequestLog()
        self.app = None
        self.load_accounts_from_file()
        logger.info(self.stats())
        self.launch_server()

    def launch_server(self):
        self.app = Flask(__name__)
        self.app.config['BASIC_AUTH_USERNAME'] = self.config.auth_username
        self.app.config['BASIC_AUTH_PASSWORD'] = self.config.auth_password
        basic_auth = BasicAuth(self.app)
        self.app.config['BASIC_AUTH_FORCE'] = True
        self.app.config['MAX_CONTENT_LENGTH'] = 16 * 1000 * 1000

        self.app.add_url_rule('/', "fallback", self.fallback, methods=['GET', 'POST'])
        self.app.add_url_rule('/<first>', "fallback", self.fallback, methods=['GET', 'POST'])
        self.app.add_url_rule('/<first>/<path:rest>', "fallback", self.fallback, methods=['GET', 'POST'])

        self.app.add_url_rule("/get-current/<device>", "get_current_account", self.get_current_account,
                              methods=['GET', 'POST'])
        self.app.add_url_rule("/get/<device>", "get_account", self.get_account, methods=['GET', 'POST'])
        self.app.add_url_rule("/get/<device>/<level>", "get_account_level", self.get_account, methods=['GET', 'POST'])
        self.app.add_url_rule("/set/level/by-device/<device>/<level>", "set_level_by_device",
                              self.set_level_by_device, methods=['GET', 'POST'])
        self.app.add_url_rule("/set/level/by-account/<account>/<level>", "set_level_by_account",
                              self.set_level_by_account, methods=['GET', 'POST'])
        self.app.add_url_rule("/set/burned/by-device/<device>", "set_burned_by_device",
                              self.set_burned_by_device, methods=['GET', 'POST'])
        self.app.add_url_rule("/set/burned/by-device/<device>/<ts>", "set_burned_by_device",
                              self.set_burned_by_device, methods=['GET', 'POST'])
        self.app.add_url_rule("/set/burned/by-account/<account>", "set_burned_by_account",
                              self.set_burned_by_account, methods=['GET', 'POST'])
        self.app.add_url_rule("/set/burned/by-account/<account>/<ts>", "set_burned_by_account",
                              self.set_burned_by_account, methods=['GET', 'POST'])
        self.app.add_url_rule("/stats", "stats", self.stats, methods=['GET'])

        werkzeug_logger = logging.getLogger("werkzeug")
        werkzeug_logger.setLevel(logging.WARNING)
        logger.info(f"start listening on port {self.port}")
        self.app.run(host=self.host, port=self.port, debug=False, use_reloader=False)

    def load_accounts_from_file(self, file="accounts.txt"):
        accounts = []
        if not os.path.isfile(file):
            logger.warning(f"{file} not found - not adding accounts")
            return False
        with open(file, "r") as f:
            for line in f:
                try:
                    split = line.strip().split(",")
                    if len(split) > 2:
                        logger.warning(f"Invalid account entry: {line}")
                        continue
                    username, password = split
                    accounts.append((username, password))
                except Exception as e:
                    logger.warning(f"{e} trying to parse: {line}")
                    continue
        sql = ("INSERT INTO accounts (username, password) VALUES (%s, %s) ON DUPLICATE KEY UPDATE "
               "password=VALUES(password);")
        logger.info(f"Loaded {len(accounts)} from {file}")
        with Db() as conn:
            conn.cur.executemany(sql, accounts)
            conn.conn.commit()
        return True

    def resp_ok(self, data=None):
        standard = {"status": "ok"}
        if data is None:
            data = standard
        if "status" not in data:
            data = {"status": "ok", "data": data}
        if not data == standard:
            logger.trace(f"responding with 200, data: {data}")
        return data, 200, self.resp_headers

    def invalid_request(self, data=None, code=400):
        if data is None:
            data = {"status": "fail"}
        if "status" not in data:
            data = {"status": "fail", "data": data}
        logger.warning(f"responding with 400, data: {data}")
        return data, code, self.resp_headers

    def fallback(self, first=None, rest=None):
        logger.warning("Fallback called")
        if request.method == 'POST':
            logger.warning(f"POST request to fallback at {first}/{rest}")
        if request.method == 'GET':
            logger.warning(f"GET request to fallback at {first}/{rest}")
        return self.invalid_request()

    def is_rate_limited(self, device=None):
        device_logger = logger.bind(name=device)
        if not device:
            return RateLimit.unknown

        # check RateLimit.burst - strict_rate_limit (quick repeated requests)
        # include usernames from the device's RequestLog into the query and choose the largest timestamp
        # - this is when the device last got any account
        latest = 0
        latest_request = f"SELECT max(last_use) FROM accounts WHERE in_use_by = \"{device}\""
        for username in self.request_log.get_logged_usernames(device):
            latest_request += f" or username = \"{username}\""
        device_logger.trace(f"{latest_request=}")
        latest = Db.get(latest_request)
        print_string = humanize.precisedelta(int(int(time.time()) - latest)) if latest > 0 else "an eternity"
        device_logger.info(f"Latest allowed request was {print_string} ago")
        # the actual check against the configured rate limit interval
        if int(time.time()) - latest < self.config.strict_rate_limit_seconds:
            device_logger.warning("Rate-limited! Device requested an account less than "
                                  f"{self.config.strict_rate_limit_minutes} minutes ago!")
            return RateLimit.burst

        # check RateLimit.period - requesting too many accounts across the configured interval
        limiting_requests: int = 0
        if device in self.request_log:
            # sorted for nicer logging - no programmatical use
            for request in sorted(self.request_log[device], key=itemgetter("ts")):
                device_logger.debug(f"Evaluating previous request: {request} "
                                    f"({humanize.precisedelta(int(time.time()) - request['ts'])} ago)")
                if request["ts"] > int(time.time()) - (self.config.rate_limit_minutes * 60):
                    device_logger.debug("Found request within rate-limit interval")
                    limiting_requests += 1

        if limiting_requests >= self.config.rate_limit_number:
            device_logger.warning(f"Rate-limited! {limiting_requests=} >= {self.config.rate_limit_number}")
            return RateLimit.period
        else:
            device_logger.trace(f"NOT rate-limited! {limiting_requests=} < {self.config.rate_limit_number}")
            return RateLimit.unlimited

    def get_account(self, device=None, level=30):
        device_logger = logger.bind(name=device)
        if not device or not can_be_type(level, int):
            return self.invalid_request()

        username = None
        pw = None

        # default select statement - can get overridden in the rate limit handler below
        last_returned_limit = self.config.get_cooldown_timestamp()
        select = (f"SELECT username, password from accounts WHERE in_use_by is NULL AND level >= {int(level)} AND "
                  f"GREATEST(last_returned, last_burned) < {last_returned_limit} ORDER BY last_use ASC LIMIT 1;")

        rate_limit_state = self.is_rate_limited(device)
        # True if RateLimit is not 0 - that would be RateLimit.unlimited
        # this if-statement chooses the correct SQL query as variable "select"
        if rate_limit_state:
            device_logger.trace("rate-limited ... handle it")
            try:
                # get the first not-burned account with suitable level from the request log
                c: int = 0
                while c < Config.rate_limit_number:
                    try:
                        previous_username = self.request_log[device][c]["username"]
                        if (not Db.is_account_burned(previous_username) and
                                Db.is_account_at_level(previous_username, level)):
                            select = f"SELECT username, password from accounts where username = \"{previous_username}\""
                            device_logger.info(f"Getting earliest queue account ({previous_username})")
                            break
                        else:
                            device_logger.debug(f"account {previous_username} unusable .. try next")
                    except Exception as e:
                        if c == 0:
                            raise IndexError("No accounts in request log")
                        continue
                    finally:
                        c += 1
                else:
                    # keep the default select statement because all accounts in the request log were burned
                    if not Config.allow_rate_limit_override_when_burned:
                        raise RuntimeError("Not allowed to override rate limit when all accounts are burned!")
                    device_logger.warning("All accounts in request log have been marked as burned - allow to get "
                                          "a fresh account despite rate-limit")
                    rate_limit_state = RateLimit.unlimited
                # rotating backwards by one item moves the first item to the end of the deque, so always the account not
                # used the longest will be returned, without changing the timestamp used to evaluate the rate limit
                # -> backward rotation is handled by RequestLog
                self.request_log.rotate(device)
            except Exception as e:
                select = f"SELECT username, password from accounts where in_use_by = \"{device}\""
                device_logger.warning(f"Unable to get a previous account ({e})- getting its current account again")
                username, pw = Db.get_elements_of_first_result(select, num=2)
        else:
            device_logger.trace(f"not rate-limited ... move on")

        if not username or not pw:
            device_logger.trace(select)
            username, pw = Db.get_elements_of_first_result(select, num=2)
        if not username or not pw:
            device_logger.error(f"Unable to return an account")
            return self.invalid_request({"error": "No accounts available"})

        # successfully got username and pw to return - now update DB to reflect the change
        reset = (f"UPDATE accounts SET in_use_by = NULL, last_returned = '{int(time.time())}' WHERE "
                 f" in_use_by = '{device}';")
        Db.execute(reset)

        # on RateLimit.burst, do not update timestamps in DB to allow to get a new account after the burst limit
        # - the burst may be justified if the device persistently retries
        if rate_limit_state != RateLimit.burst:
            device_logger.debug(f"{rate_limit_state=} - update timestamps in DB")
            mark_used = (f"UPDATE accounts SET in_use_by = '{device}', last_use = '{int(time.time())}' WHERE "
                         f"username = '{username}';")
        else:
            device_logger.debug(f"{rate_limit_state=} - do not update timestamps in DB")
            mark_used = f"UPDATE accounts SET in_use_by = '{device}' WHERE username = '{username}';"
        Db.execute(mark_used)

        # make sure every account is only added to the RequestLog once
        if device not in self.request_log or username not in self.request_log.get_logged_usernames(device):
            log_entry: dict = {"ts": int(time.time()), "username": username}
            device_logger.debug(f"log this request: {log_entry}")
            self.request_log.log(device, log_entry)
        else:
            device_logger.debug(f"NOT log this request")

        device_logger.info(f"return {username=}, {pw=}")
        device_logger.info(f"{self.stats()}\n")
        # newline for visual separation of requests ...
        print()
        return self.resp_ok({"username": username, "password": pw})

    def set_level_by_account(self, account=None, level=None):
        logger.info(f"Set level by account: {account=} to {level=}")
        if not (level and account) or not can_be_type(level, int):
            return self.invalid_request()
        sql = f"UPDATE accounts SET level = {int(level)} WHERE username = \"{account}\""
        Db.execute(sql)
        return self.resp_ok()

    def set_level_by_device(self, device=None, level=None):
        # find the assigned account, then return self.set_level_by_account
        device_logger = logger.bind(name=device)
        device_logger.info(f"Set level by device to {level=}")
        if not (device and level) or not can_be_type(level, int):
            return self.invalid_request()
        sql = f"SELECT username FROM accounts WHERE in_use_by = \"{device}\""
        username = Db.get(sql)
        if username:
            return self.set_level_by_account(account=username, level=level)
        return self.invalid_request()

    def set_burned_by_account(self, account=None, ts=int(time.time())):
        logger.info(f"Set burned by account: {account=} at {ts=}")
        if not (account and ts) or not can_be_type(ts, int):
            return self.invalid_request()
        sql = f"UPDATE accounts SET last_burned = {int(ts)} WHERE username = \"{account}\""
        Db.execute(sql)
        return self.resp_ok()

    def set_burned_by_device(self, device=None, ts=int(time.time())):
        # find the assigned account, then return self.set_burned_by_account
        device_logger = logger.bind(name=device)
        device_logger.info(f"Set burned by device at {ts=}")
        if not (device and ts) or not can_be_type(ts, int):
            return self.invalid_request()
        sql = f"SELECT username FROM accounts WHERE in_use_by = \"{device}\""
        username = Db.get(sql)
        if username:
            return self.set_burned_by_account(account=username, ts=ts)
        return self.invalid_request()

    def get_current_account(self, device=None):
        device_logger = logger.bind(name=device)
        device_logger.info("Get current account")
        if not device:
            return self.invalid_request()
        sql = f"SELECT username FROM accounts WHERE in_use_by = \"{device}\""
        username = Db.get(sql)
        if username:
            data = {"username": username}
            device_logger.info(f"Return current account: {data}")
            return self.resp_ok(data)

    def force_release(self):
        sql = (f"UPDATE accounts SET in_use_by = NULL, last_returned = '{int(time.time())}' WHERE "
               f"in_use_by IS NOT NULL AND last_returned < {int(time.time()) - self.config.force_release_seconds}")
        sql_log = (f"SELECT * FROM accounts WHERE in_use_by IS NOT NULL AND last_returned < "
                   f"{int(time.time()) - self.config.force_release_seconds} ORDER BY last_returned DESC")
        with Db() as conn:
            conn.cur.execute(sql_log)
            for res in conn.cur:
                logger.info(f"Force release this account after {int(self.config.force_release_seconds / 60 / 60 / 24)}"
                            f" days: {res}")
        Db.execute(sql)
        return True

    def stats(self):
        self.force_release()
        last_returned_limit = self.config.get_cooldown_timestamp()

        cd_sql = f"SELECT count(*) from accounts WHERE GREATEST(last_returned, last_burned) >= {last_returned_limit}"
        in_use_sql = "SELECT count(*) from accounts WHERE in_use_by IS NOT NULL"
        total_sql = "SELECT count(*) from accounts"

        self.cd, self.in_use, self.total = Db.get_single_results(cd_sql, in_use_sql, total_sql)
        self.available = self.total - self.in_use - self.cd
        try:
            self.accs_per_device = round(self.total / self.in_use, 2)
            self.required_per_device = round((self.in_use + self.cd) / self.in_use, 2)
            self.hours_per_account = round(24 / self.required_per_device, 2)
        except ZeroDivisionError:
            logger.warning("Encountered ZeroDivisionError trying to calculate stats ... return zeroes")
            self.accs_per_device = 0
            self.required_per_device = 0
            self.hours_per_account = 0

        return {"accounts": self.total, "accounts_per_device": self.accs_per_device,
                "required_per_device": self.required_per_device, "hours_per_account": self.hours_per_account,
                "in_use": self.in_use, "cooldown": self.cd, "available": self.available}


if __name__ == "__main__":
    serv = AccountServer()
    while True:
        time.sleep(1)
