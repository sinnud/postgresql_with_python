import unittest
from unittest import TestCase
from app.postgresql import PGConnect
import logzero
import traceback # Python error trace
from logzero import logger

# Set a minimum log level
import logging
#logzero.loglevel(logging.INFO)

class UT_postgresql(TestCase):
    def setUp(self):
        #logger.debug('setUp in UT_postgresql...')
        self.cls = PGConnect()
        #logger.debug('setUp in UT_postgresql')

    def tearDown(self):
        #logger.debug('tearDown in UT_postgresql...')
        self.cls.__del__()
        #logger.debug('tearDown in UT_postgresql')

    def test_postgresql_connection(self):
        #logger.debug('test_postgresql_connection...')
        rst = self.cls.execute('select count(*) from schm_sgl.tbl_huge')
        self.assertEqual(rst[0][0], 0)
        #logger.debug('test_postgresql_connection')

if __name__ == '__main__':
    unittest.main()