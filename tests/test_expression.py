import unittest

from pandasdb import DataBase
from pandasdb.table import Table
from pandasdb.column import Column

DB_FILE = '../data/forestation.db'


class TestExpression(unittest.TestCase):
    def setUp(self) -> None:
        self.db = DataBase(DB_FILE, block_till_ready=True)
        self.table: Table = self.db[self.db.tables[0]]
        self.column: Column = getattr(self.table, self.table.columns[0])

    def tearDown(self) -> None:
        self.db.exit()

    def test_and(self):
        pass

    def test_or(self):
        pass

    def test_repr(self):
        self.assertIsInstance(repr(self.column), str)
        self.assertIsInstance(str(self.column), str)


if __name__ == '__main__':
    unittest.main()
