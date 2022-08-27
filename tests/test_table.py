from pandas import DataFrame, Series

import unittest
from collections.abc import Generator

from src import DataBase
from src.table import Table, Column, Expression
from src.exceptions import InvalidColumnError


DB_FILE = '../data/forestation.db'
SQL_FILE = '../data/parch-and-posey.sql'
SQLITE_FILE = '../data/mental_health.sqlite'
MAIN_DATABASE = DB_FILE

MIN_TABLES = 1
MIN_COLUMNS = 3  # for the first table


class TestExpression(unittest.TestCase):
    def setUp(self) -> None:
        self.db = DataBase(MAIN_DATABASE)
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


class TestColumn(unittest.TestCase):
    def setUp(self) -> None:
        self.db = DataBase(MAIN_DATABASE)
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
        self.db = DataBase(MAIN_DATABASE)
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
            rows = len(cursor.execute(self.table.query).fetchall())

        self.assertIsInstance(self.table.len, int)
        self.assertNotEqual(rows, 0)
        self.assertEqual(self.table.len, rows)
        self.assertEqual(self.table.len, len(self.table))

    def test_shape(self):
        with self.table.conn as cursor:
            first_row = next(cursor.execute(self.table.query))

        shape = self.table.len, len(first_row)
        self.assertEqual(self.table.shape, shape)

    def test_to_df(self):
        df = self.table.to_df()
        self.assertIsInstance(df, DataFrame)
        self.assertEqual(df.shape, self.table.shape)
        self.assertEqual(list(df.columns), self.table.columns)

        with self.table.conn as cursor:
            db_first_row = cursor.execute(self.table.query).fetchone()

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
