from pandas import Series

import unittest
from collections.abc import Generator

from src import DataBase
from src.table import Table
from src.column import Column
from src.expression import Expression


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
                n_rows = len(cursor.execute(self.table.query).fetchall())

            self.assertEqual(n_rows, length)

    def test_to_series(self):
        out = self.column.to_series()
        self.assertIsInstance(out, Series)
        self.assertEqual(out.size, self.column.len)
        self.assertEqual(out.name, self.column._name)

        with self.db.conn as cursor:
            query = next(cursor.execute(self.column.query))[0]  # returns tuple, ex: ('AMD',)
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
        self.assertGreaterEqual(len(self.column), 5,
                                msg='First table must have at least 5 rows to complete this test')
        out = self.column.iloc(0)
        self.assertIsInstance(out, (str, int, float))

        index = []
        self.assertRaisesRegex(
            TypeError,
            f'Index must be of type int, not: {str(list)}',
            self.column.iloc, index
        )
        index = 32.32
        self.assertRaisesRegex(
            TypeError,
            f'Index must be of type int, not: {str(float)}',
            self.column.iloc, index
        )

        index = self.column.len
        self.assertRaisesRegex(
            IndexError,
            'Given index is out of range',
            self.column.iloc, index
        )

        index = (self.column.len + 1) * -1  # to convert to negative
        self.assertRaisesRegex(
            IndexError,
            'Given index is out of range',
            self.column.iloc, index
        )

        last_elem_idx = self.column.len - 1
        self.assertEqual(
            self.column.iloc(last_elem_idx),
            self.column.iloc(-1))

        # assert not raises error
        self.column.iloc(self.column.len - 1)
        self.column.iloc(self.column.len * -1)
        self.column.iloc(0)
        self.column.iloc(1)
        self.column.iloc(3)
        self.column.iloc(-1)
        self.column.iloc(-3)

    def test_iter(self):
        self.assertIsInstance(iter(self.column), Generator)

        for val, _ in zip(self.column, range(5)):
            self.assertNotIsInstance(val, tuple)

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
