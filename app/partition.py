from postgresql import PGConnect
import logzero
import traceback # Python error trace
from logzero import logger

# Set a minimum log level
import logging
logzero.loglevel(logging.INFO)

from datetime import date, time, datetime, timedelta
from random import randint

class PartitionInitial(object):
    def create_truncate_table(self
                              , dbname='dbhuge', schema='tbl_sgl'
                              , tbl_name = 'tbl_huge'
                              , tbl_str = 'tid BIGINT, inserteddatetime TIMESTAMP, randomdata TEXT'
                              , truncate_flag = True
                              , drop_flag = True
                            ):
        try:
            pgc0=PGConnect(dbname=dbname)
            rst=pgc0.execute(f"SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = '{schema}')")
            if not rst[0][0]:
                pgc0.execute(f'create schema {schema}')
            pgc=PGConnect(dbname=dbname, schema=schema)
            rst=pgc.execute(f"SELECT to_regclass('{schema}.{tbl_name}')")
            if not rst[0][0]:
                pgc.execute(f'create table {schema}.{tbl_name} ({tbl_str})')
                return True
            qry='select column_name, data_type'
            qry=f"{qry} from INFORMATION_SCHEMA.COLUMNS where"
            qry=f"{qry} table_schema = '{schema}' AND table_name = '{tbl_name}'"
            rst=pgc.execute(qry)
            str=', '.join([' '.join(x) for x in rst])
            str=str.replace('timestamp without time zone', 'timestamp')
            if str.lower() == tbl_str.lower():
                if truncate_flag:
                    pgc.execute(f'truncate table {schema}.{tbl_name}')
                return True
            logger.error(f"Table '{schema}.{tbl_name}' exist with different structure!")
            logger.debug(f"Existing table structure: {str}")
            logger.debug(f"provided structure: {tbl_str}")
            if drop_flag:
                pgc.execute(f'drop table {schema}.{tbl_name}')
                pgc.execute(f'create table {schema}.{tbl_name} ({tbl_str})')
                return True
            return False
        except:
            logger.error(f"{traceback.format_exc()}")
            return False

    def create_random_staging_table(self, dbname=None, schema=None
                                    , tbl_name=None, tbl_str=None
                                    , pos_date=None, insdt=None, num_txn=None
                                    ):
        try:
            self.create_truncate_table(dbname=dbname, schema=schema, tbl_name=tbl_name, tbl_str=tbl_str)
            qry = f'with t as (select generate_series(1,{num_txn}) AS id'
            qry = f"{qry}\n,('{pos_date.strftime('%y%m%d')}'"
            qry = f"{qry}\n||lpad(mod(abs(('x'||substr(md5("
            qry = f"{qry}random()::text||random()::text||random()::text||random()::text"
            qry = f"{qry}),1,16))::bit(64)::bigint)"
            qry = f"{qry}\n,10000000000000)::text,13,'0'))::bigint as tid"
            qry = f"{qry}\n, '{insdt.strftime('%Y%m%d %X')}'::timestamp as inserteddatetime"
            qry = f"{qry}\n, md5(random()::text)::text AS randomdata)"
            qry = f"{qry}\n insert into {schema}.{tbl_name} select tid, inserteddatetime, randomdata from t"
            pgc = PGConnect(dbname=dbname, schema=schema)
            rst = pgc.execute(qry)
            qry=f"delete from {schema}.{tbl_name} a"
            qry = f"{qry}\n using (select tid, max(randomdata) as maxdata"
            qry = f"{qry}   from {schema}.{tbl_name} group by tid having count(*)>1) b"
            qry = f"{qry}\nwhere a.tid=b.tid"
            qry = f"{qry}\n and a.randomdata<b.maxdata;"
            rst = pgc.execute(qry)
            return True
        except:
            logger.error(f"{traceback.format_exc()}")
            return False

    def update_single_table(self, dbname=None, schema=None
                            , tbl_cum=None, tbl_stg=None):
        try:
            pgc = PGConnect(dbname=dbname, schema=schema)
            qry = f"delete from {schema}.{tbl_cum}"
            qry = f"{qry}\n where tid in (select tid from {schema}.{tbl_stg})"
            rst = pgc.execute(qry)
            qry = f"insert into {schema}.{tbl_cum}"
            qry = f"{qry}\n select * from {schema}.{tbl_stg}"
            rst = pgc.execute(qry)
            return True
        except:
            logger.error(f"{traceback.format_exc()}")
            return False

    def sim_load_one_day_data(self, dbname=None, schema=None, pos_date=None
                            , tbl_cum=None, tbl_stg=None):
        thisdate=pos_date
        thistime = time(6, 0, 0)
        startdt=datetime.combine(thisdate, thistime)
        thisdt=startdt
        stopdt=datetime.combine(thisdate, time(22, 0, 0))
        runtime=0
        while thisdt <= stopdt:
            num_txn = randint(20000, 40000)
            rst=self.create_random_staging_table(dbname=dbname, schema=schema
                                , tbl_name = tbl_stg
                                , tbl_str = 'tid bigint, inserteddatetime timestamp, randomdata text'
                                                , pos_date=thisdate
                                                , insdt=thisdt
                                                , num_txn=num_txn
                                                )
            rst=self.update_single_table(dbname=dbname, schema=schema
                                , tbl_cum=tbl_cum, tbl_stg=tbl_stg)
            runtime = runtime + 1
            thisdt = thisdt + timedelta(minutes=5)
        return True