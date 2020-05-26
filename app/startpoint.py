import os
import sys
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

def main(arg=None):
    pi=PartitionInitial()
    startdate=date(2019, 1, 1)
    enddate=date(2019, 2, 1)
    thisdate=startdate
    while thisdate < enddate:
        logger.info(f"Start loading {thisdate} data...")
        pi.sim_load_one_day_data(dbname='dbhuge', schema='tbl_sgl', pos_date=thisdate
                        , tbl_cum='tbl_huge', tbl_stg='tbl_stg')
        thisdate = thisdate + timedelta(days=1)
if __name__ == '__main__':
    mylog=os.path.realpath(__file__).replace('.py','.log')
    if os.path.exists(mylog):
        os.remove(mylog)
    logzero.logfile(mylog)

    logger.info(f'start python code {__file__}.\n')
    if len(sys.argv)>1:
        logger.info(f"Argument: {sys.argv}")
        myarg=sys.argv
        pgmname=myarg.pop(0)
        logger.info(f"Program name:{pgmname}.")
        logger.info(f"Arguments:{myarg}.")
        main(arg=' '.join(myarg))
    else:
        logger.info(f"No arguments")
        main(arg=None)
    logger.info(f'end python code {__file__}.\n')
