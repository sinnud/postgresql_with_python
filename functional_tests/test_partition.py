import sys, os
testdir = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.abspath(os.path.join(testdir, '..', 'app')))

#print(sys.path)

import unittest
from unittest import TestCase
from postgresql import PGConnect
from partition import PartitionInitial
import logzero
import traceback # Python error trace
from logzero import logger

# Set a minimum log level
import logging
#logzero.loglevel(logging.INFO)

from datetime import date, time, datetime, timedelta
from random import randint

class UT_postgresql(TestCase):
    def postgresql_connect(self):
        return PGConnect()
                       # host='localhost'
                       # , dbname='dbhuge'
                       # , schema='public'
                       # , user='sinnud'
                       # , password='Jeffery45!@'

    def test_check_schema_exists(self):
        try:
            schema_name='schm_sgl'
            pgc = self.postgresql_connect()
            rst=pgc.execute(f"SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = '{schema_name}')")
            self.assertEqual(rst[0][0], True)
            return rst[0][0]
        except:
            logger.error(f"{traceback.format_exc()}")
            return None

    def test_create_schema_schm_sgl_if_not_exist(self):
        try:
            if not self.test_check_schema_exists():
                pgc = self.postgresql_connect()
                pgc.execute('create schema schm_sgl')
        except:
            logger.error(f"{traceback.format_exc()}")
    '''
    def test_check_schema_table_exists(self):
        try:
            table_name='schm_sgl.tbl_huge'
            pgc = self.postgresql_connect()
            rst=pgc.execute(f"SELECT to_regclass('{table_name}')")
            self.assertEqual(rst[0][0], table_name)
            if rst[0][0] is None:
                return rst[0][0]
            return True
        except:
            logger.error(f"{traceback.format_exc()}")
            return None
    
    def test_create_table_tbl_huge_under_schema_schm_sgl(self):
        try:
            if not self.test_check_schema_table_exists():
                pgc = self.postgresql_connect()
                qry = 'create table schm_sgl.tbl_huge (tid bigint, inserteddatetime timestamp, randomdata text)'
                pgc.execute(qry)
        except:
            logger.error(f"{traceback.format_exc()}")
    '''
    def test_show_structure_table_tbl_huge_under_schema_schm_sgl(self):
        try:
            schema='schm_sgl'
            tbl_name='tbl_huge'
            pgc = self.postgresql_connect()
            qry = 'select column_name, data_type'
            qry = f"{qry} from INFORMATION_SCHEMA.COLUMNS where"
            qry = f"{qry} table_schema = '{schema}' AND table_name = '{tbl_name}'"
            rst=pgc.execute(qry)
            logger.debug(rst)
            strlist=[' '.join(x) for x in rst]
            logger.debug(strlist)
            str=', '.join([' '.join(x) for x in rst])
            logger.debug(str)
            str=str.replace('timestamp without time zone', 'timestamp')
            logger.debug(str)
            str=str.lower()
            logger.debug(str)
        except:
            logger.error(f"{traceback.format_exc()}")
    '''
    def test_create_single_table_tbl_huge_under_schema_schm_sgl(self):
        pi=PartitionInitial()
        rst=pi.create_truncate_table(dbname='dbhuge', schema='tbl_sgl'
                            , tbl_name = 'tbl_huge'
                            , tbl_str = 'tid bigint, inserteddatetime timestamp, randomdata text')
        self.assertIs(rst, True)
    
    def test_insert_random_data_into_tbl_stg_under_schema_schm_sgl(self):
        pi=PartitionInitial()
        rst=pi.create_truncate_table(dbname='dbhuge', schema='tbl_sgl'
                            , tbl_name = 'tbl_stg'
                            , tbl_str = 'tid bigint, inserteddatetime timestamp, randomdata text')
        pgc = self.postgresql_connect()
        thisdate = date(2019, 1, 1)
        thistime = time(8, 0, 0)
        #self.assertEqual(thisdate, date.today())
        #dt=datetime.now()
        #self.assertEqual(thistime, time(dt.hour, dt.minute, dt.second))
        thisdt=datetime.combine(thisdate, thistime)
        #thisdt = thisdt + timedelta(minutes=5)
        #self.assertEqual(thisdt, datetime.now())
        num_txn = randint(20, 30)
        qry = f'with t as (select generate_series(1,{num_txn}) AS id'
        qry = f"{qry}\n,('{thisdate.strftime('%y%m%d')}'"
        qry = f"{qry}\n||lpad(mod(abs(('x'||substr(md5(random()::text),1,16))::bit(64)::bigint)"
        qry = f"{qry}\n,10000000000000)::text,13,'0'))::bigint as tid"
        qry = f"{qry}\n, '{thisdt.strftime('%Y%m%d %X')}'::timestamp as inserteddatetime"
        qry = f"{qry}\n, md5(random()::text)::text AS randomdata)"
        qry = f"{qry}\n insert into tbl_sgl.tbl_stg select tid, inserteddatetime, randomdata from t"
        rst = pgc.execute(qry)
        #self.assertEqual(rst[0][0], False)
    
    def test_insert_into_tbl_huge_from_tbl_stg_under_schema_schm_sgl(self):
        pi=PartitionInitial()
        thisdate = date(2019, 1, 1)
        thistime = time(8, 0, 0)
        thisdt=datetime.combine(thisdate, thistime)
        num_txn = randint(290, 300)
        rst=pi.create_random_staging_table(dbname='dbhuge', schema='tbl_sgl'
                            , tbl_name = 'tbl_stg'
                            , tbl_str = 'tid bigint, inserteddatetime timestamp, randomdata text'
                                            , pos_date=thisdate
                                            , insdt=thisdt
                                            , num_txn=num_txn
                                            )
        rst=pi.update_single_table(dbname='dbhuge', schema='tbl_sgl'
                            , tbl_cum='tbl_huge', tbl_stg='tbl_stg')
    
    def test_insert_one_day_data_to_tbl_huge_under_schema_schm_sgl(self):
        pi=PartitionInitial()
        thisdate = date(2019, 1, 1)
        thistime = time(6, 0, 0)
        startdt=datetime.combine(thisdate, thistime)
        thisdt=startdt
        stopdt=datetime.combine(thisdate, time(22, 0, 0))
        runtime=0
        while True:
            num_txn = randint(20000, 40000)
            rst=pi.create_random_staging_table(dbname='dbhuge', schema='tbl_sgl'
                                , tbl_name = 'tbl_stg'
                                , tbl_str = 'tid bigint, inserteddatetime timestamp, randomdata text'
                                                , pos_date=thisdate
                                                , insdt=thisdt
                                                , num_txn=num_txn
                                                )
            rst=pi.update_single_table(dbname='dbhuge', schema='tbl_sgl'
                                , tbl_cum='tbl_huge', tbl_stg='tbl_stg')
            runtime = runtime + 1
            thisdt = thisdt + timedelta(minutes=5)
            self.assertLess(thisdt, stopdt, f'Access {stopdt} from {startdt} through {runtime} times')
    
    def test_insert_one_day_data_to_tbl_huge_under_schema_schm_sgl(self):
        pi=PartitionInitial()
        thisdate = date(2019, 2, 1)
        pi.sim_load_one_day_data(dbname='dbhuge', schema='tbl_sgl', pos_date=thisdate
                            , tbl_cum='tbl_huge', tbl_stg='tbl_stg')
    '''
    def test_insert_one_month_data_to_tbl_huge_under_schema_schm_sgl(self):
        pi=PartitionInitial()
        startdate=date(2019, 1, 1)
        enddate=date(2019, 2, 1)
        thisdate=startdate
        while thisdate < enddate:
            pi.sim_load_one_day_data(dbname='dbhuge', schema='tbl_sgl', pos_date=thisdate
                            , tbl_cum='tbl_huge', tbl_stg='tbl_stg')
            thisdate = thisdate + timedelta(days=1)
if __name__ == '__main__':
    unittest.main()