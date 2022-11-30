from pandas import DataFrame

import unittest
from collections.abc import Generator

from pandasdb import Database
from pandasdb.table import IndexLoc, Table, TableView
from pandasdb.column import Column
from pandasdb.exceptions import InvalidColumnError
from pandasdb.utils import get_random_name


DB_FILE = '../data/forestation.db'
SQL_FILE = '../data/parch-and-posey.sql'
SQLITE_FILE = '../data/mental_health.sqlite'
MAIN_DATABASE = DB_FILE

MIN_TABLES = 1
MIN_COLUMNS = 3  # for the first table


class TestTable(unittest.TestCase):
    def setUp(self) -> None:
        self.db = Database(MAIN_DATABASE, cache=False)
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

    def test_query(self):
        out = self.db.regions.query
        table_query = 'SELECT country_name, country_code, region, income_group FROM regions'
        self.assertNotIn(member='_rowid_', container=out)
        self.assertNotIn(member='rowid', container=out)
        self.assertEqual(out, table_query)

        tbl = self.db.regions
        table_view = tbl[tbl.country_code.isin(['ITA', 'GPY', 'USA'])]
        out = table_view.query
        self.assertIsInstance(table_view, TableView)
        self.assertNotIn(member='_rowid_', container=out)
        self.assertNotIn(member='rowid', container=out)

        filtered_view = table_view[table_view.income_group == 'High income']  # filter the previous view
        out = filtered_view.query
        self.assertIsInstance(table_view, TableView)
        self.assertNotIn(member='_rowid_', container=out)
        self.assertNotIn(member='rowid', container=out)

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
        self.assertEqual(self.table.shape, self.table.to_df().shape)

    def test_describe(self):
        for _, table in self.db.items():
            data = {}
            for name, col in table.items():
                data[name] = col.describe()

            self.assertIsInstance(data, dict)
            self.assertEqual(len(data), len(table.columns))
            self.assertEqual(table.describe(), data)

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

    def test_sample(self):
        out = self.table.sample(n=10)
        self.assertIsInstance(out, list)
        self.assertEqual(len(out), 10)

        random_samples = [self.table.sample() for _ in range(5)]
        unique_items = (random_samples.count(x) == 1 for x in random_samples)
        self.assertTrue(all(unique_items))

    def test_items(self):
        self.assertIsInstance(self.table.items(), Generator)

        for col_name, col in self.table.items():
            self.assertIsInstance(col_name, str)
            self.assertIsInstance(col, Column)

            # try accessing data for the column
            self.assertIsInstance(col.len, int)
            self.assertIsInstance(col.sql_type, str)
            data = col.data(5)
            self.assertIsInstance(data, list)
            self.assertEqual(len(data), 5)

    def test_applymap(self):
        for table_name in self.db.tables:
            table = self.db[table_name].applymap(
                lambda x: len(str(x)) if x is None or type(x) in (str, float) else x,
                ignore_na=False
            )

            for row in table:
                self.assertIsInstance(row, tuple)

                for item in row:
                    self.assertIsInstance(item, int)

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

        out = self.table.iloc[len(self.table) + 5:]
        self.assertIsInstance(out, list)
        self.assertEqual(len(out), 0)

        types = [dict(), set(), tuple(), 3.32, '3.32']
        for i in types:
            self.assertRaisesRegex(
                TypeError,
                f'Index must be of type: int, list, or slice. not: {type(i)}',
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

    def test_filter(self):
        tbl = self.db.regions
        out = tbl.filter(tbl.income_group == 'Low income')

        self.assertEqual(len(self.db.temp_views), 1)
        self.assertEqual(out.shape[1], tbl.shape[1])
        self.assertEqual(out.columns, tbl.columns)
        self.assertTrue(len(out) < len(tbl))
        self.assertEqual(len(out), 34)

        # filter the filtered table
        out2 = out.filter(out.region == 'Sub-Saharan Africa')
        self.assertEqual(out2.shape[1], tbl.shape[1])
        self.assertEqual(out2.columns, tbl.columns)
        self.assertTrue(len(out2) < len(out))
        self.assertEqual(len(out2), 27)

        out3 = out.filter(out.region.like('Sub-Saharan Africa'))
        self.assertTrue(out3.equals(out2))

        out4 = out.filter(out.region.like('Sub-Saharan Africa'.lower()))
        self.assertTrue(out4.equals(out3))

        combined_filter = tbl.filter((tbl.income_group == 'Low income') & (tbl.region.like('Sub-Saharan Africa')))
        self.assertTrue(combined_filter.equals(out2))

        filter_using_getitem = tbl[(tbl.income_group == 'Low income') & (tbl.region.like('Sub-Saharan Africa'))]
        self.assertTrue(filter_using_getitem.equals(combined_filter))

    def test_sort_values(self):
        tbl: Table = self.db.forest_area
        out = tbl.sort_values(column='year', ascending=True)
        self.assertIsInstance(out, Table)
        self.assertIsInstance(out, TableView)
        self.assertEqual(tbl.shape, out.shape)
        self.assertEqual(tbl.columns, out.columns)

        query = f'SELECT year FROM {out.name} WHERE year IS NOT NULL'
        with out.conn as cur:
            year_col: list[int] = [tup[0] for tup in cur.execute(query)]
        self.assertEqual(year_col, sorted(year_col))  # assert its sorted correctly

        tbl.sort_values(column='year', ascending=False)
        tbl.sort_values(column=['country_code', 'country_name', 'year'])
        tbl.sort_values(column={'country_code': 'ASC', 'country_name': 'DESC', 'year': 'ASC'})
        out = tbl.sort_values(column={'forest_area_sqkm': 'DESC', 'country_code': 'ASC', 'year': 'ASC'})
        self.assertEqual(tbl.shape, out.shape)
        self.assertEqual(tbl.columns, out.columns)

    def test_limit(self):
        table = self.table
        limit_table = self.table.limit(25)

        self.assertIsInstance(limit_table, TableView)
        self.assertEqual(len(limit_table), 25)
        self.assertEqual(limit_table.columns, table.columns)

        for a, b in zip(limit_table, table):
            self.assertEqual(a, b)

        # try using the _rowid_ column with iloc:
        self.assertEqual(limit_table.iloc[0], table.iloc[0])
        self.assertEqual(limit_table.iloc[-1], table.iloc[24])
        self.assertEqual(next(iter(limit_table)), next(iter(table)))

    def test_create_and_get_temp_view(self):
        tbl: Table = self.db.regions

        view_name = f'test_view_{get_random_name()}'
        query = f'SELECT * FROM {tbl.name} LIMIT 20'
        self.assertRaisesRegex(
            ValueError,
            'Query must alias the rowid column as `_rowid_` for `iloc` to work.',
            tbl._create_and_get_temp_view, view_name=view_name, query=query
        )

        n_temp_views = len(self.db.temp_views)
        query = f'SELECT rowid AS _rowid_, * FROM {tbl.name} LIMIT 20'
        out = tbl._create_and_get_temp_view(view_name=view_name, query=query)
        self.assertEqual(n_temp_views, len(self.db.temp_views) - 1)  # adds a new temporary view

        self.assertIsInstance(out, TableView)
        self.assertEqual(len(out), 20)
        self.assertEqual(out.columns, tbl.columns)
        self.assertEqual(next(iter(out)), next(iter(tbl)))
        self.assertEqual(out.iloc[0], tbl.iloc[0])
        self.assertEqual(out.iloc[19], tbl.iloc[19])

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
        non_existing_column = get_random_name(10)
        self.assertRaisesRegex(
            InvalidColumnError,
            f'^Column must be one of the following: {", ".join(self.table.columns)}$',
            self.table._get_col, non_existing_column
        )

        for col in self.table.columns:
            col_obj = self.table._get_col(column=col)
            self.assertEqual(col_obj.name, col)

            attr_col = getattr(self.table, col)
            item_col = self.table[col]
            self.assertEqual(id(attr_col), id(item_col))

            self.assertEqual(id(col_obj), id(attr_col))

    def test_getitem(self):
        tbl = self.db.forest_area
        filtered_table = tbl[tbl.country_code == 'ITA']
        self.assertIsInstance(filtered_table, TableView)

        col_name = 'country_name'
        out = tbl[col_name]
        self.assertIsInstance(out, Column)

        non_existent_column = get_random_name(10)
        self.assertRaises(
            KeyError,
            tbl.__getitem__, non_existent_column
        )

        item = 42
        self.assertRaisesRegex(
            TypeError,
            f'Argument must be of type str or Expression. not: {type(item)}',
            tbl.__getitem__, item
        )

    def test_hash(self):
        self.assertIsInstance(hash(self.table), int)

    def test_repr_df(self):
        for name, table in self.db.items():
            df = table._repr_df()

            self.assertIsInstance(df, DataFrame)

    def test_repr(self):
        self.assertIsInstance(repr(self.table), str)
        self.assertIsInstance(str(self.table), str)

    def test_repr_html_(self):
        self.assertIsInstance(self.table._repr_html_(), str)

    def test_equals(self):
        db = self.db
        self.assertFalse(db.forest_area.equals(db.land_area))
        self.assertFalse(db.forest_area.equals(db.regions))

        self.assertTrue(db.forest_area.equals(db.forest_area))
        self.assertTrue(db.regions.equals(db.regions.limit(10000)))


class TestTableView(unittest.TestCase):
    def setUp(self) -> None:
        self.db = Database(MAIN_DATABASE, cache=False)
        self.table: Table = self.db[self.db.tables[0]]

        tables = self.db.tables
        self.assertGreaterEqual(len(tables), MIN_TABLES,
                                msg='Database must have at least one table for the tests')

        self.assertGreaterEqual(len(self.table.columns), MIN_COLUMNS,
                                msg='Database must have at least 3 columns in the first table for the tests')

    def tearDown(self) -> None:
        self.db.exit()

    def test_columns(self):
        table: Table = self.db.regions
        table_view = table.limit(10)
        view_cols = table_view.columns
        table_cols = table.columns
        self.assertNotIn(member='_rowid_', container=view_cols)
        self.assertNotIn(member='rowid', container=view_cols)
        self.assertEqual(view_cols, table_cols)

        nested_view = table_view.limit(10)
        self.assertIsInstance(table_view, TableView)
        self.assertNotIn(member='_rowid_', container=nested_view.columns)
        self.assertNotIn(member='rowid', container=nested_view.columns)
        self.assertEqual(nested_view.columns, table.columns)

        nested_view = nested_view.limit(10)
        self.assertIsInstance(table_view, TableView)
        self.assertNotIn(member='_rowid_', container=nested_view.columns)
        self.assertNotIn(member='rowid', container=nested_view.columns)
        self.assertEqual(nested_view.columns, table.columns)


if __name__ == '__main__':
    unittest.main()
