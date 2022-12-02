from pandas import Series, DataFrame
import numpy as np

import unittest
from collections.abc import Generator

from pandasdb import Database
from pandasdb.table import Table
from pandasdb.column import Column, ColumnView
from pandasdb.expression import Expression
from pandasdb.utils import sort_iterable_with_none_values, get_random_name, convert_type_to_sql


DB_FILE = '../data/forestation.db'

MIN_TABLES = 1
MIN_COLUMNS = 3  # for the first table


# TODO replace all nested for loops with this
def col_iterator(db: Database, *, numeric_only: bool) -> Generator[Column, None, None]:
    """ Generator that yields all the columns (objects) from all tables """
    for _, table in db.items():
        for _, col in table.items():
            if numeric_only:
                if col.data_is_numeric():
                    yield col
            else:
                yield col


class TestColumn(unittest.TestCase):
    def setUp(self) -> None:
        self.db = Database(DB_FILE, cache=False)
        self.table: Table = self.db[self.db.tables[0]]
        column = self.table.columns[0]
        self.column: Column = self.table[column]

    def tearDown(self) -> None:
        self.db.exit()

    def test_type(self):
        for name, col in self.table.items():
            out = col.type
            self.assertIsInstance(out, type)
            self.assertIn(out, (str, int, float))

    def test_sql_type(self):
        for name, col in self.table.items():
            out = col.sql_type
            self.assertIsInstance(out, str)
            self.assertGreater(len(out), 0)

    def test_data_is_numeric(self):
        for name, col in self.table.items():
            is_numeric = col.data_is_numeric()
            self.assertIsInstance(is_numeric, bool)

            first_val = next(iter(col))
            if is_numeric:
                self.assertIsInstance(first_val, (int, float))
            else:
                self.assertIsInstance(first_val, str)

    def test_len(self):
        for name, col in self.table.items():
            length = col.len
            self.assertIsInstance(length, int)
            self.assertGreater(length, 0)

            with self.db.conn as cursor:
                n_rows = len(cursor.execute(self.table.query).fetchall())

            self.assertEqual(n_rows, length)
            self.assertEqual(col.len, col.count() + col.null_count())

    def test_count(self):
        for name, col in self.table.items():
            out = col.count()
            self.assertIsInstance(out, int)
            self.assertGreater(out, 0)
            self.assertEqual(col.count() + col.null_count(), col.len)

    def test_na_count(self):
        for name, col in self.table.items():
            out = col.null_count()
            self.assertIsInstance(out, int)
            self.assertEqual(col.null_count() + col.count(), col.len)

            c = 0
            for x in col:
                if x is None:
                    c += 1
            self.assertEqual(c, out)

    def test_min(self):
        for name, col in self.table.items():
            col_min = col.min()
            ser_min = col.to_series().min()
            self.assertEqual(ser_min, col_min)

    def test_max(self):
        for name, col in self.table.items():
            col_max = col.max()
            ser_max = col.to_series().max()
            self.assertEqual(ser_max, col_max)

    def test_sum(self):
        for name, col in self.table.items():
            if col.data_is_numeric():
                col_sum = col.sum()
                ser_sum = col.to_series().sum()
                self.assertAlmostEqual(ser_sum, col_sum, places=4)  # SQLite SUM() rounds to 4
            else:
                self.assertRaisesRegex(
                    TypeError,
                    f'Cannot get sum for Column of type {col.type}',
                    col.sum
                )

    def test_avg(self):
        for name, col in self.table.items():
            if col.data_is_numeric():
                col_avg = col.avg()
                ser_avg = col.to_series().mean()
                self.assertAlmostEqual(ser_avg, col_avg, places=4)  # round to 4 for consistency
            else:
                self.assertRaisesRegex(
                    TypeError,
                    f'Cannot get avg for Column of type {col.type}',
                    col.avg
                )

    def test_median(self):
        # TODO add tests for columns with len of 4 and 5
        for table in self.db.tables:
            for name, col in self.db[table].items():
                if col.data_is_numeric():
                    col_median = col.median()
                    ser_median = col.to_series().median()
                    self.assertAlmostEqual(ser_median, col_median, places=4)
                else:
                    self.assertRaisesRegex(
                        TypeError,
                        f'Cannot get median for Column of type {col.type}',
                        col.median
                    )

    def test_mode(self):
        for name, col in self.table.items():
            out = col.mode()
            self.assertIsInstance(out, dict)
            self.assertGreater(len(out), 0)

            lst = list(out.values())
            self.assertEqual(lst.count(lst[0]), len(lst))  # assert all values are the same

            if col.type in (str, int):
                ser_mode = col.to_series().mode().to_dict()
                # convert to list because type(dict_values) is never equal to type(dict_keys)
                self.assertEqual(list(ser_mode.values()), list(out.keys()))

    def test_describe(self):
        for name, col in self.table.items():
            col_dict: dict[str, float] = col.describe()
            ser: Series = col.to_series()

            if col.data_is_numeric():
                d = {
                    col_dict['len']: len(ser),
                    col_dict['count']: ser.count(),
                    col_dict['min']: ser.min(),
                    col_dict['max']: ser.max(),
                    col_dict['sum']: ser.sum(),
                    col_dict['avg']: ser.mean(),
                    col_dict['median']: ser.median()
                }
            else:
                d = {
                    col_dict['len']: len(ser),
                    col_dict['count']: ser.count(),
                    col_dict['min']: ser.min(),
                    col_dict['max']: ser.max(),
                    col_dict['unique']: len(ser.unique())
                }

            for key, val in d.items():
                if isinstance(key, float):
                    key = round(key, 4)
                    val = round(val, 4)
                self.assertEqual(key, val)

    def test_unique(self):
        for name, col in self.table.items():
            col_unique = col.unique()
            ser_unique = col.to_series().unique()
            self.assertEqual(len(col_unique), len(ser_unique))

            for x, y in zip(col_unique, ser_unique):
                if x is None:
                    self.assertTrue(np.isnan(y))
                else:
                    self.assertEqual(x, y)

    def test_value_counts(self):
        for name, col in self.table.items():
            col_vc = col.value_counts()
            ser_vc = col.to_series().value_counts().to_dict()

            self.assertEqual(len(col_vc), len(ser_vc))
            self.assertEqual(col_vc, ser_vc)

    def test_to_series(self):
        out = self.column.to_series()
        self.assertIsInstance(out, Series)
        self.assertEqual(out.size, self.column.len)
        self.assertEqual(out.name, self.column.name)

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

    def test_sample(self):
        out = self.column.sample()
        self.assertIsInstance(out, list)
        self.assertNotIsInstance(out[0], (list, tuple))

        a = self.column.sample(10)
        b = self.column.sample(10)
        self.assertEqual(len(a), len(b))
        self.assertEqual(len(a), 10)
        self.assertNotEqual(a, b)

    def test_apply(self):
        for name, col in self.table.items():

            if col.type is int:
                it = col.apply(lambda x: len(str(x)))
                for cell in it:
                    if cell is not None:
                        self.assertIsInstance(cell, int)
                        self.assertGreaterEqual(cell, 1)

            elif col.type is float:
                col1 = col.apply(round, args=(1,))
                col2 = col.apply(round, ndigits=1)  # test args and kwargs

                for cell1, cell2 in zip(col1, col2):
                    if cell1 is None:
                        continue

                    self.assertIsInstance(cell1, float)
                    self.assertIsInstance(cell2, float)
                    self.assertEqual(cell1, cell2)

                    decimals = str(cell1).split('.')[-1]
                    self.assertEqual(len(decimals), 1)

                    decimals = str(cell2).split('.')[-1]
                    self.assertEqual(len(decimals), 1)
                    
            elif col.type is str:
                it = col.apply(lambda x: x.split()[-1])
                for cell in it:
                    if cell is not None:
                        self.assertNotIn(member=' ', container=cell)

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

        out = self.column.iloc[len(self.column) + 5:]
        self.assertIsInstance(out, list)
        self.assertEqual(len(out), 0)

        types = [dict(), set(), tuple(), 3.32, '3.32']
        for i in types:
            self.assertRaisesRegex(
                TypeError,
                f'Index must be of type: int, list, or slice. not: {type(i)}',
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

    def test_not_null(self):
        for _, table in self.db.items():
            for _, col in table.items():
                null_count = col.null_count()
                if null_count == 0:
                    self.assertEqual(len(col), len(col.not_null()))
                    self.assertFalse(any(x is None for x in col))
                else:
                    self.assertTrue(any(x is None for x in col))
                    self.assertFalse(any(x is None for x in col.not_null()))
                    self.assertEqual(len(col), len(col.not_null()) + null_count)

    def test_sort_values(self):
        for _, table in self.db.items():
            for _, col in table.items():
                py_sorted_col = sort_iterable_with_none_values(col)
                sql_sorted_col = list(col.sort_values())
                self.assertEqual(len(py_sorted_col), len(sql_sorted_col))
                self.assertEqual(py_sorted_col, sql_sorted_col)

    def test_limit(self):
        for i in (0, 1, 2, 5, 10, 50, 100):
            out = self.column.limit(i)
            self.assertEqual(len(out), i)
            sliced_column: list = self.column.iloc[:i]
            self.assertEqual(len(sliced_column), len(out))
            self.assertEqual(sliced_column, list(out))

    def test_filter(self):
        self.assertTrue(
            DB_FILE.endswith('forestation.db'),
            'Database must be forestation.db for the following test to work'
        )
        df = self.db.forest_area
        name = 'Aruba'
        filtered_col = df.country_name.filter(df.country_name == name)

        self.assertTrue(len(filtered_col) < len(df))
        self.assertTrue(set(filtered_col).issubset(df.country_name))

    def test_create_and_get_temp_view(self):
        name = f'test_view_{get_random_name(10)}'
        query = f'SELECT _rowid_, {self.column.name} FROM {self.column.table} LIMIT 10'
        self.assertRaisesRegex(
            ValueError,
            'Query must alias the rowid column as `_rowid_` for `iloc` to work.',
            self.column._create_and_get_temp_view, view_name=name, query=query
        )

        n_temp_views = len(self.db.temp_views)
        query = f'SELECT _rowid_ AS _rowid_, {self.column.name} FROM {self.column.table} LIMIT 10'
        out = self.column._create_and_get_temp_view(view_name=name, query=query)
        col = self.column
        self.assertGreater(len(self.db.temp_views), n_temp_views)

        self.assertIsInstance(out, ColumnView)
        self.assertEqual(len(out), 10)
        self.assertEqual(next(iter(out)), next(iter(col)))
        self.assertEqual(out.iloc[0], col.iloc[0])
        self.assertEqual(out.iloc[9], col.iloc[9])

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

        self.assertTrue(
            DB_FILE.endswith('forestation.db'),
            'Database must be forestation.db for the following test to work'
        )
        df = self.db.forest_area
        name = 'Aruba'
        filtered_col = df.country_name[df.country_name == name]

        self.assertTrue(len(filtered_col) < len(df))
        self.assertTrue(set(filtered_col).issubset(df.country_name))

    def test_iter(self):
        self.assertIsInstance(iter(self.column), Generator)

        for val, _ in zip(self.column, range(5)):
            self.assertNotIsInstance(val, (tuple, list))

    def test_hash(self):
        self.assertIsInstance(hash(self.column), int)

    def test_repr_df(self):
        df = self.table._repr_df()
        self.assertIsInstance(df, DataFrame)
        self.assertEqual(len(df), 20)

    def test_repr(self):
        self.assertIsInstance(repr(self.column), str)
        self.assertIsInstance(str(self.column), str)

    def test_repr_html_(self):
        self.assertIsInstance(self.column._repr_html_(), str)


class TestColumnLogicalOp(unittest.TestCase):
    """
    Test logical operators for Column objects (db.table.col >= 20, db.table.col.between(10, 25))
    """
    def setUp(self) -> None:
        self.db = Database(DB_FILE, block_till_ready=True)
        self.table: Table = self.db[self.db.tables[0]]
        self.column: Column = getattr(self.table, self.table.columns[0])

    def tearDown(self) -> None:
        self.db.exit()

    def test_add(self):
        df = self.db.forest_area
        new_col = df.year + df.year
        for a, b in zip(new_col, df.year):
            self.assertEqual(a, b + b)

        col = self.db.forest_area.forest_area_sqkm
        it = (2 for _ in range(len(col)))
        new_col = col + it
        for a, b in zip(new_col, col):
            if a is not None:
                self.assertEqual(a, b + 2)

        for a, b in zip(col + 2.05, col):
            if a is not None:
                self.assertEqual(a, b + 2.05)

        col = self.db.land_area.country_name
        s = ' - Country name'
        for idx, x in enumerate(col + s):
            if idx == 0:
                self.assertEqual(x, 'Aruba - Country name')
            if x is not None:
                self.assertTrue(x.endswith(s))

        col = self.db.land_area.total_area_sq_mi
        for a, b in zip(col + True, col):
            if a is not None:
                self.assertEqual(a, b + True)

    def test_sub(self):
        df = self.db.forest_area
        new_col = df.year - df.year
        for a, b in zip(new_col, df.year):
            self.assertEqual(a, b - b)

        col = self.db.forest_area.forest_area_sqkm
        it = (2 for _ in range(len(col)))
        new_col = col - it
        for a, b in zip(new_col, col):
            if a is not None:
                self.assertEqual(a, b - 2)

        for a, b in zip(col - 2.05, col):
            if a is not None:
                self.assertEqual(a, b - 2.05)

    def test_mul(self):
        col = self.db.forest_area.forest_area_sqkm

        it = (3 for _ in range(len(col)))
        for a, b in zip(col, col * it):
            if a is not None:
                self.assertEqual(a * 3, b)

        for a, b in zip(col, col * 1.25):
            if a is not None:
                self.assertEqual(a * 1.25, b)

    def test_truediv(self):
        col = self.db.forest_area.forest_area_sqkm

        it = (21.3 for _ in range(len(col)))
        for a, b in zip(col, col / it):
            if a is not None:
                self.assertEqual(a / 21.3, b)

        for a, b in zip(col, col / 1.25):
            if a is not None:
                self.assertEqual(a / 1.25, b)

    def test_floordiv(self):
        col = self.db.forest_area.forest_area_sqkm

        it = (3.3 for _ in range(len(col)))
        for a, b in zip(col, col // it):
            if a is not None:
                self.assertEqual(a // 3.3, b)

        for a, b in zip(col, col // 0.75):
            if a is not None:
                self.assertEqual(a // 0.75, b)

    # TODO: finish Expression tests
    def test_gt(self):
        for col in col_iterator(self.db, numeric_only=True):
            median = col.median()
            exp = col > median
            filtered_col = col[exp]
            n_filtered_col = len(filtered_col)

            self.assertIsInstance(exp, Expression)
            self.assertEqual(exp.query, f'{col.name} > {median}')
            self.assertLess(n_filtered_col, len(col))
            self.assertTrue(all(val > median for val in filtered_col))

    def test_ge(self):
        for col in col_iterator(self.db, numeric_only=True):
            median = col.median()
            exp = col >= median
            filtered_col = col[exp]
            n_filtered_col = len(filtered_col)

            self.assertIsInstance(exp, Expression)
            self.assertEqual(exp.query, f'{col.name} >= {median}')
            self.assertLess(n_filtered_col, len(col))
            self.assertTrue(all(val >= median for val in filtered_col))

    def test_lt(self):
        for col in col_iterator(self.db, numeric_only=True):
            median = col.median()
            exp = col < median
            filtered_col = col[exp]
            n_filtered_col = len(filtered_col)

            self.assertIsInstance(exp, Expression)
            self.assertEqual(exp.query, f'{col.name} < {median}')
            self.assertLess(n_filtered_col, len(col))
            self.assertTrue(all(val < median for val in filtered_col))

    def test_le(self):
        for col in col_iterator(self.db, numeric_only=True):
            median = col.median()
            exp = col <= median
            filtered_col = col[exp]
            n_filtered_col = len(filtered_col)

            self.assertIsInstance(exp, Expression)
            self.assertEqual(exp.query, f'{col.name} <= {median}')
            self.assertLess(n_filtered_col, len(col))
            self.assertTrue(all(val <= median for val in filtered_col))

    def test_eq(self):
        for col in col_iterator(self.db, numeric_only=False):
            mode = next(iter(col.mode().keys()))
            exp = col == mode
            filt_col = col[exp]

            self.assertIsInstance(exp, Expression)
            if mode is None:
                self.assertEqual(exp.query, f'{col.name} IS NULL')
                self.assertTrue(all(x is None for x in filt_col))
            else:
                self.assertEqual(exp.query, f'{col.name} = {convert_type_to_sql(mode)}')
                self.assertTrue(all(x == mode for x in filt_col))

    def test_ne(self):
        for col in col_iterator(self.db, numeric_only=False):
            mode = next(iter(col.mode().keys()))
            exp = col != mode
            filt_col = col[exp]

            self.assertIsInstance(exp, Expression)
            if mode is None:
                self.assertEqual(exp.query, f'{col.name} IS NOT NULL')
                self.assertTrue(all(x is not None for x in filt_col))
            else:
                self.assertEqual(exp.query, f'{col.name} != {convert_type_to_sql(mode)}')
                self.assertTrue(all(x != mode for x in filt_col))

    def test_isin(self):
        for col in col_iterator(self.db, numeric_only=False):
            pass

    def test_between(self):
        for col in col_iterator(self.db, numeric_only=True):
            pass

    def test_like(self):
        for col in col_iterator(self.db, numeric_only=False):
            pass

    # SQLite3 doesn't support it yet
    # def test_ilike(self):
    #     pass


if __name__ == '__main__':
    unittest.main()
