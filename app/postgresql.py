import psycopg2
from logzero import logger
import time

class PGConnect(object):
    def __init__(self, host='localhost', dbname='dbhuge', schema='public', user='sinnud', password='Jeffery45!@'):
        self.host = host
        self.dbname = dbname
        self.schema = schema
        self.user = user
        self.password = password
        self.conn_string = f"host='{self.host}' dbname='{self.dbname}' user='{self.user}' password='{self.password}'"
        self.conn = None

    def __repr__(self):
        return f"PostgreSQL: host='{self.host}' dbname='{self.dbname}'"

    def __del__(self):
        if self.conn:
            self.conn.close()
            logger.debug(f"Connection to database '{self.host}' closed")

    def cursor(self):
        if not self.conn or self.conn.closed or self.conn.cursor().closed:
            self.connect()
        return self.conn.cursor()

    def connect(self):
        logger.debug(f"Connection to '{self.host}' postgresql on database '{self.dbname}'...")
        self.conn = psycopg2.connect(self.conn_string)
        self.conn.set_session(autocommit=True)
        self.conn.set_client_encoding('UTF8')

    def execute(self, query):
        if not self.conn or self.conn.closed:
            self.connect()
        start = time.time()
        cursor = self.conn.cursor()
        cursor.execute(query)
        log_message = f'Executed query:\n\n {query}  \n\n' \
            f'Rows affected : {cursor.rowcount}, took {time.time() - start} seconds.'
        logger.debug(log_message)

        try:
            rst = cursor.fetchall()
        except Exception as e:
            return cursor
        if len(rst) < 1000:
            logger.debug(f'# Query result : {rst}.')
        return rst
