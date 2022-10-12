import unittest
import random

from pandasdb.utils import *

DB_FILE = '../data/forestation.db'
SQL_FILE = '../data/parch-and-posey.sql'
SQLITE_FILE = '../data/mental_health.sqlite'


class TestUtils(unittest.TestCase):
    def test_same_val_generator(self):
        val = random.random()
        size = 9

        it = same_val_generator(val=val, size=size)
        for x in it:
            self.assertEqual(x, val)

        it = same_val_generator(val=val, size=size)
        self.assertEqual(len(tuple(it)), size)

    def test_infinite_generator(self):
        val = random.random()
        it = infinite_generator(val=val)
        
        for x, _ in zip(it, range(100)):
            self.assertEqual(x, val)

    def test_concat(self):
        pass

    def test_mb_size(self):
        pass

    def test_rename_duplicate_cols(self):
        pass

    def test_convert_db_to_sql(self):
        pass

    def test_convert_csvs_to_db(self):
        pass

    def test_convert_sql_to_db(self):
        pass

    def test_load_sql_to_sqlite(self):
        pass


if __name__ == '__main__':
    unittest.main()
