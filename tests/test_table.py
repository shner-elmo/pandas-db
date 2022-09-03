from pandas import DataFrame

import unittest
from collections.abc import Generator

from src import DataBase
from src.table import Table
from src.column import Column
from src.indexloc import IndexLoc
from src.exceptions import InvalidColumnError


DB_FILE = '../data/forestation.db'
SQL_FILE = '../data/parch-and-posey.sql'
SQLITE_FILE = '../data/mental_health.sqlite'
MAIN_DATABASE = DB_FILE

MIN_TABLES = 1
MIN_COLUMNS = 3  # for the first table


class TestTable(unittest.TestCase):
    def setUp(self) -> None:
        self.db = DataBase(MAIN_DATABASE)
        self.table: Table = self.db[self.db.tables[0]]

        tables = self.db.tables
        self.assertGreaterEqual(len(tables), MIN_TABLES,
                                msg='Database must have at least one table for the tests')

        self.assertGreaterEqual(len(self.table.columns), MIN_COLUMNS,
                                msg='Database must have at least 3 columns in the first table for the tests')

    def tearDown(self) -> None:
        self.db.exit()

    def test_init(self):
        for col in self.table.columns:
            self.assertTrue(expr=hasattr(self.table, col),
                            msg=f'Columns: {col} not in Table attributes')

    def test_columns(self):
        self.assertTrue(len(self.table.columns) >= 3)
        for col_name in self.table.columns:
            self.assertIsInstance(col_name, str)

    def test_len(self):
        with self.table.conn as cursor:
            rows = len(cursor.execute(self.table._query).fetchall())

        self.assertIsInstance(self.table.len, int)
        self.assertNotEqual(rows, 0)
        self.assertEqual(self.table.len, rows)
        self.assertEqual(self.table.len, len(self.table))

    def test_shape(self):
        with self.table.conn as cursor:
            first_row = next(cursor.execute(self.table._query))

        shape = self.table.len, len(first_row)
        self.assertEqual(self.table.shape, shape)

    def test_to_df(self):
        df = self.table.to_df()
        self.assertIsInstance(df, DataFrame)
        self.assertEqual(df.shape, self.table.shape)
        self.assertEqual(list(df.columns), self.table.columns)

        with self.table.conn as cursor:
            db_first_row = cursor.execute(self.table._query).fetchone()

        df_first_row = tuple(df.iloc[0])
        self.assertEqual(df_first_row, db_first_row)

    def test_data(self):
        data = self.table.data()
        first_row = data[0]

        self.assertIsInstance(data, list)
        self.assertEqual(len(data), self.table.len)
        self.assertEqual(len(first_row), len(self.table.columns))

        data = self.table.data(5)
        self.assertEqual(len(data), 5)

    def test_items(self):
        self.assertIsInstance(self.table.items(), Generator)

        for col_name, col in self.table.items():
            self.assertIsInstance(col_name, str)
            self.assertIsInstance(col, Column)

            # try accessing data for the column
            self.assertIsInstance(col.len, int)
            self.assertIsInstance(col.type, str)
            data = col.data(5)
            self.assertIsInstance(data, list)
            self.assertEqual(len(data), 5)

    def test_iloc(self):
        """
        Test all three ways to get an index slice: int, list, and slice
        """
        self.assertGreaterEqual(len(self.table), 30,
                                msg='First table must have at least 30 rows to complete this test')

        out = self.table.iloc
        self.assertIsInstance(out, IndexLoc)

        out = self.table.iloc[0]
        self.assertIsInstance(out, tuple)
        self.assertEqual(len(out), len(self.table.columns))

        out = self.table.iloc[3]
        self.assertIsInstance(out, tuple)
        self.assertEqual(len(out), len(self.table.columns))

        out = self.table.iloc[-1]
        self.assertIsInstance(out, tuple)
        self.assertEqual(len(out), len(self.table.columns))

        last_row_idx = len(self.table) - 1
        self.assertEqual(self.table.iloc[last_row_idx], self.table.iloc[-1])

        lst = [3, 5, 3, -1]
        out = self.table.iloc[lst]
        self.assertIsInstance(out, list)
        self.assertEqual(len(out), len(lst))

        lst = [3, -1, 5, 3, -1]
        out = self.table.iloc[lst]
        self.assertIsInstance(out, list)
        self.assertEqual(len(out), len(lst))

        out = self.table.iloc[:]
        self.assertIsInstance(out, list)
        self.assertEqual(len(out), len(self.table))

        out = self.table.iloc[:5]
        self.assertIsInstance(out, list)
        self.assertEqual(len(out), 5)

        out = self.table.iloc[3:]
        self.assertIsInstance(out, list)
        self.assertEqual(len(out), len(self.table) - 3)

        out = self.table.iloc[3:8]
        self.assertIsInstance(out, list)
        self.assertEqual(len(out), 5)

        out = self.table.iloc[2:24:2]
        self.assertIsInstance(out, list)
        self.assertEqual(len(out), 11)

        types = [dict(), set(), tuple(), 3.32, '3.32']
        for i in types:
            self.assertRaisesRegex(
                TypeError,
                f'Index must be of type: int, list, or slice, not: {type(i)}',
                self.table.iloc.__getitem__, i
            )

        index = self.table.len
        self.assertRaisesRegex(
            IndexError,
            'Given index out of range',
            self.table.iloc.__getitem__, index
        )

        index = (self.table.len + 1) * -1  # to convert to negative
        self.assertRaisesRegex(
            IndexError,
            'Given index out of range',
            self.table.iloc.__getitem__, index
        )

    def test_iter(self):
        self.assertIsInstance(iter(self.table), Generator)

        for row, _ in zip(self.table, range(5)):
            self.assertIsInstance(row, tuple)
            self.assertEqual(len(row), len(self.table.columns))

    def test_get_col(self):
        """
        Table.__getitem__() and Table.__getattr__()
        are two different ways to get the Column object,
        they both call Table._get_col() to get the object which is stored
        as an attribute, and consequently they both return the same preexisting
        Column object (set in __init__()).

        If a requested Column isn't present in the Table, InvalidColumnError is raised
        """
        non_existing_column = 'abcd fgh'
        self.assertRaisesRegex(
            InvalidColumnError,
            f'^Column must be one of the following: {", ".join(self.table.columns)}$',
            self.table._get_col, non_existing_column
        )

        for col in self.table.columns:
            col_obj = self.table._get_col(column=col)
            self.assertEqual(col_obj._name, col)

            attr_col = getattr(self.table, col)
            item_col = self.table[col]
            self.assertEqual(attr_col, item_col)

            self.assertEqual(col_obj, attr_col)

    def test_hash(self):
        self.assertIsInstance(hash(self.table), int)

    def test_repr(self):
        self.assertIsInstance(repr(self.table), str)
        self.assertIsInstance(str(self.table), str)


if __name__ == '__main__':
    unittest.main()
