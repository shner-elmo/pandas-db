from pandas import Series

import unittest
from collections.abc import Generator

from pandasdb import DataBase
from pandasdb.table import Table
from pandasdb.column import Column
from pandasdb.expression import Expression


DB_FILE = '../data/forestation.db'

MIN_TABLES = 1
MIN_COLUMNS = 3  # for the first table


class TestColumn(unittest.TestCase):
    def setUp(self) -> None:
        self.db = DataBase(DB_FILE)
        self.table: Table = self.db[self.db.tables[0]]
        self.column: Column = getattr(self.table, self.table.columns[0])

    def tearDown(self) -> None:
        self.db.exit()

    def test_type(self):
        for name, col in self.table.items():
            out = col.type
            self.assertIsInstance(out, str)
            self.assertGreater(len(out), 0)

    def test_len(self):
        for name, col in self.table.items():
            length = col.len
            self.assertIsInstance(length, int)
            self.assertGreater(length, 0)

            with self.db.conn as cursor:
                n_rows = len(cursor.execute(self.table._query).fetchall())

            self.assertEqual(n_rows, length)

    def test_to_series(self):
        out = self.column.to_series()
        self.assertIsInstance(out, Series)
        self.assertEqual(out.size, self.column.len)
        self.assertEqual(out.name, self.column._name)

        with self.db.conn as cursor:
            query = next(cursor.execute(self.column._query))[0]  # returns tuple, ex: ('AMD',)
        ser = out.iloc[0]
        col = next(iter(self.column))

        self.assertEqual(ser, col)
        self.assertEqual(ser, query)

    def test_data(self):
        data = self.column.data()
        self.assertIsInstance(data, list)
        self.assertEqual(len(data), self.column.len)

        data = self.column.data(5)
        self.assertEqual(len(data), 5)

    def test_iloc(self):
        """
        Test all three ways to get an index slice: int, list, and slice        
        """
        self.assertGreaterEqual(len(self.table), 30,
                                msg='First table must have at least 30 rows to complete this test')

        out = self.column.iloc[0]
        self.assertNotIsInstance(out, (list, tuple))

        out = self.column.iloc[3]
        self.assertNotIsInstance(out, (list, tuple))

        out = self.column.iloc[-1]
        self.assertNotIsInstance(out, (list, tuple))

        last_row_idx = len(self.column) - 1
        self.assertEqual(self.column.iloc[last_row_idx], self.column.iloc[-1])

        lst = [3, 5, 3, -1]
        out = self.column.iloc[lst]
        self.assertIsInstance(out, list)
        self.assertEqual(len(out), len(lst))

        lst = [3, -1, 5, 3, -1]
        out = self.column.iloc[lst]
        self.assertIsInstance(out, list)
        self.assertEqual(len(out), len(lst))

        out = self.column.iloc[:]
        self.assertIsInstance(out, list)
        self.assertEqual(len(out), len(self.column))

        out = self.column.iloc[:5]
        self.assertIsInstance(out, list)
        self.assertEqual(len(out), 5)

        out = self.column.iloc[3:]
        self.assertIsInstance(out, list)
        self.assertEqual(len(out), len(self.column) - 3)

        out = self.column.iloc[3:8]
        self.assertIsInstance(out, list)
        self.assertEqual(len(out), 5)

        out = self.column.iloc[2:24:2]
        self.assertIsInstance(out, list)
        self.assertEqual(len(out), 11)

        types = [dict(), set(), tuple(), 3.32, '3.32']
        for i in types:
            self.assertRaisesRegex(
                TypeError,
                f'Index must be of type: int, list, or slice, not: {type(i)}',
                self.column.iloc.__getitem__, i
            )

        index = self.column.len
        self.assertRaisesRegex(
            IndexError,
            'Given index out of range',
            self.column.iloc.__getitem__, index
        )

        index = (self.column.len + 1) * -1  # to convert to negative
        self.assertRaisesRegex(
            IndexError,
            'Given index out of range',
            self.column.iloc.__getitem__, index
        )

    def test_getitem(self):
        """
        There are two ways of getting a slice from a Column object;
        from the iloc property, ex: Column.iloc[-5], or: Column[-5]
        """
        self.test_iloc()

        out = self.column[-1]
        self.assertNotIsInstance(out, (list, tuple))

        lst = [3, -1, 5, 3, -1]
        out = self.column[lst]
        self.assertIsInstance(out, list)
        self.assertEqual(len(out), len(lst))

        out = self.column[2:24:2]
        self.assertIsInstance(out, list)
        self.assertEqual(len(out), 11)

    def test_iter(self):
        self.assertIsInstance(iter(self.column), Generator)

        for val, _ in zip(self.column, range(5)):
            self.assertNotIsInstance(val, (tuple, list))

    def test_hash(self):
        self.assertIsInstance(hash(self.column), int)

    def test_repr(self):
        self.assertIsInstance(repr(self.column), str)
        self.assertIsInstance(str(self.column), str)


class TestColumnLogicalOp(unittest.TestCase):
    """
    Test logical operators for Column objects (db.table.col >= 20, db.table.col.between(10, 25))
    """
    def setUp(self) -> None:
        self.db = DataBase(DB_FILE)
        self.table: Table = self.db[self.db.tables[0]]
        self.column: Column = getattr(self.table, self.table.columns[0])
        self.col: str = f'{self.column._table}.{self.column._name}'

    def tearDown(self) -> None:
        self.db.exit()

    def test_gt(self):
        exp = self.column > 12.32
        self.assertIsInstance(exp, Expression)
        self.assertEqual(exp.query, self.col + f' > 12.32 ')

    # TODO: finish Expression tests
    def test_ge(self):
        pass

    def test_lt(self):
        pass

    def test_le(self):
        pass

    def test_eq(self):
        pass

    def test_ne(self):
        pass

    def test_isin(self):
        pass

    def test_between(self):
        pass

    def test_like(self):
        pass

    def test_ilike(self):
        pass


if __name__ == '__main__':
    unittest.main()
