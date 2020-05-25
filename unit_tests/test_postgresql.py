import unittest
from unittest import TestCase
from app.postgresql import PGConnect

class UT_postgresql(TestCase):
    def setUp(self):
        self.cls = PGConnect()

    def tearDown(self):
        self.cls.__del__()

    def test_postgresql_connection(self):
        rst = self.cls.execute('select count(*) from schm_sgl.tbl_huge')

if __name__ == '__main__':
    unittest.main()