import mysql.connector
from config import Config


class DbConnection:
    # autocommit to always wait for queries to finish?
    # https://stackoverflow.com/a/54752005
    __config = {
        "host": Config.db_host,
        "port": Config.db_port,
        "user": Config.db_user,
        "passwd": Config.db_pw,
        "database": Config.db,
        "autocommit": True
    }

    def __init__(self):
        self.conn = mysql.connector.connect(**self.__config)
        self.cur = self.conn.cursor()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cur.close()
        try:
            self.conn.commit()
        except Exception as e:
            print(f"commit on exit failed: {e}")
        self.conn.close()

    def cursor(self, *args, **kwargs):
        return self.conn.cursor(*args, **kwargs)

    @classmethod
    def get_single_results(cls, *sqls):
        res: list = []
        with cls() as conn:
            for sql in sqls:
                conn.cur.execute(sql)
                # get the first element of the cursor (tuple) - or if it's none, get a list [None]
                # then return the first element of that result (the actual query result, or None)
                # https://stackoverflow.com/a/68186597
                res.append(next(conn.cur, [None])[0])
        return res

    @classmethod
    def execute(cls, sql):
        with cls() as conn:
            conn.cur.execute(sql)

    @classmethod
    def get_elements_of_first_result(cls, sql, num=None):
        if not "limit" in sql.lower():
            sql = sql.rstrip(";")
            sql += " LIMIT 1"
        with cls() as conn:
            conn.cur.execute(sql)
            for elem in conn.cur:
                if not num:
                    if len(elem) == 0:
                        return False
                    elif len(elem) == 1:
                        return elem[0]
                    return [e for e in elem]
                else:
                    if num == 1:
                        return elem[0] if elem[0] else False
                    c: int = 0
                    ret: list = []
                    while c < num:
                        try:
                            ret.append(elem[c])
                        except Exception as e:
                            ret.append(False)
                        c+=1
                    return ret

