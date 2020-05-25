import unittest
from unittest import TestCase
from app.postgresql import PGConnect
import logzero
import traceback # Python error trace
from logzero import logger

class UT_postgresql(TestCase):
    def postgresql_connect(self):
        return PGConnect(host='localhost'
                       , dbname='dbhuge'
                       , schema='public'
                       , user='sinnud'
                       , password='Jeffery45!@'
                       )

    def test_create_schema_schm_sgl(self):
        try:
            pgc = self.postgresql_connect()
            pgc.execute('create schema schm_sgl')
        except:
            logger.error(f"{traceback.format_exc()}")

    def test_create_table_tbl_huge_under_schema_schm_sgl(self):
        try:
            pgc = self.postgresql_connect()
            pgc.execute('create table schm_sgl.tbl_huge (tid bigint, inserteddatetime timestamp, randomdata text)')
        except:
            logger.error(f"{traceback.format_exc()}")

if __name__ == '__main__':
    unittest.main()