from pandas import DataFrame

import unittest
import sqlite3
from collections.abc import Generator

from pandasdb import Database
from pandasdb.table import Table
from pandasdb.exceptions import FileTypeError, ConnectionClosedWarning
from pandasdb.utils import create_temp_view

DB_FILE = '../data/forestation.db'
SQL_FILE = '../data/parch-and-posey.sql'
SQLITE_FILE = '../data/mental_health.sqlite'
MAIN_Database = DB_FILE

MIN_TABLES = 1


class TestConnection(unittest.TestCase):
    def setUp(self):
        self.db = Database(MAIN_Database, cache=True, populate_cache=True)

        tables = self.db.tables
        self.assertGreaterEqual(len(tables), MIN_TABLES,
                                msg='Database must have at least one table for the tests')

    def tearDown(self):
        self.db.exit()

    def test_init(self):
        valid_extension = ('.sql', '.db', '.sqlite', '.sqlite3')

        self.assertRaisesRegex(
            FileTypeError,
            f'File extension must be one of the following: {", ".join(valid_extension)}',
            Database, db_path='my_db.txt'
        )
        self.assertRaisesRegex(
            FileTypeError,
            f'File extension must be one of the following: {", ".join(valid_extension)}',
            Database, db_path='my_db.csv'
        )

        # test file type sql:
        db = Database(SQL_FILE)
        self.assertListEqual(db.tables, ['web_events', 'sales_reps', 'region', 'orders', 'accounts'])
        db.exit()

        # run same test again after creating/caching .db file
        db = Database(SQL_FILE)
        self.assertListEqual(db.tables, ['web_events', 'sales_reps', 'region', 'orders', 'accounts'])
        db.exit()

        # test file type db:
        db = Database(DB_FILE)
        self.assertListEqual(db.tables, ['forest_area', 'land_area', 'regions'])
        db.exit()

        # test file type sqlite:
        db = Database(SQLITE_FILE)
        self.assertListEqual(db.tables, ['Answer', 'Question', 'Survey'])
        db.exit()

    def test_tables(self):
        out = self.db.tables
        self.assertIsInstance(out, list)
        self.assertIsInstance(next(iter(out)), str)
        self.assertGreaterEqual(len(out), MIN_TABLES)

        tables = set(self.db.tables)
        views = set(self.db.views)
        shared_items = tables & views
        self.assertEqual(len(shared_items), 0)

    def test_views(self):
        out = self.db.views
        self.assertIsInstance(out, list)

        tables = set(self.db.tables)
        views = set(self.db.views)
        shared_items = tables & views
        self.assertEqual(len(shared_items), 0)

    def test_temp_tables(self):
        out = self.db.temp_tables
        self.assertIsInstance(out, list)
        self.assertEqual(len(out), 0)  # should be empty right after creating the SQL connection

    def test_temp_views(self):
        db = Database(MAIN_Database, cache=False)
        out = db.temp_views
        self.assertIsInstance(out, list)
        self.assertEqual(len(out), 0)  # should be empty right after creating the SQL connection

        name = 'test_view_1'
        query = f'SELECT * FROM {db.tables[0]} LIMIT 50'
        create_temp_view(conn=db.conn, view_name=name, query=query, drop_if_exists=False)
        self.assertIn(member=name, container=db.temp_views)

        with db.conn as cur:
            cur.execute(f'DROP VIEW {name}')
        self.assertNotIn(member=name, container=db.temp_views)

    def test_get_columns(self):
        out = self.db.get_columns(self.db.tables[0])
        self.assertIsInstance(out, list)
        self.assertGreaterEqual(len(out), MIN_TABLES)

    def test_items(self):
        out = self.db.items()
        self.assertIsInstance(out, Generator)

        for table_name, table_object in self.db.items():
            self.assertIsInstance(table_name, str)
            self.assertIsInstance(table_object, Table)

    def test_query(self):
        self.assertEqual(MAIN_Database, '../data/forestation.db',
                         msg="This test works only on this specific Database (forestation.db)")
        query = """
        SELECT * FROM forest_area
        JOIN regions
        ON regions.country_code = forest_area.country_code
        AND regions.country_name = forest_area.country_name"""

        df = self.db.query(sql_query=query)
        self.assertIsInstance(df, DataFrame)

        table_cols = self.db.forest_area.columns + self.db.regions.columns
        self.assertEqual(len(df.columns), len(table_cols))
        self.assertFalse(df.columns.has_duplicates)
        self.assertTrue(all(x in df.columns for x in table_cols))
        renamed_cols = ['country_code', 'country_name', 'year', 'forest_area_sqkm',
                        'country_name_2', 'country_code_2', 'region', 'income_group']
        self.assertEqual(df.columns.to_list(), renamed_cols)

    def test_context_manager(self):
        with Database(MAIN_Database) as data_base:
            table = data_base.tables[0]
            self.assertIsInstance(data_base, Database)

        self.assertRaisesRegex(
            sqlite3.ProgrammingError,
            '^Cannot operate on a closed database.$',
            data_base.query, f"SELECT * FROM {table}"
        )

    def test_exit(self):
        db = Database(MAIN_Database)
        table = db.tables[0]
        db.exit()

        self.assertRaisesRegex(
            sqlite3.ProgrammingError,
            '^Cannot operate on a closed database.$',
            db.query, f"SELECT * FROM {table}"
        )
        self.assertWarnsRegex(
            ConnectionClosedWarning,
            'Connection already closed!',
            db.exit
        )

    def test_set_table(self):
        tables = self.db.tables
        for table in tables:
            self.assertIn(member=table, container=self.db._table_items)
            self.assertIsInstance(self.db._table_items[table], Table)

        self.assertTrue(hasattr(self.db, 'conn'))
        self.assertIsInstance(self.db.conn, sqlite3.Connection)
        self.assertNotIn(member='conn', container=self.db._table_items)

        self.db._set_table(table='conn')
        self.assertIn(member='conn', container=self.db._table_items)
        self.assertIsInstance(self.db.conn, sqlite3.Connection)  # make sure we don't overwrite pre-existing attributes

    def test_get_table(self):
        """
        All the table objects are stored in self._table_items (structure: dict[str, Table])
        which is a dictionary, similarly to a Pandas Dataframe you can access the tables both
        as attributes and from __getitem__.
        note that the table will be available as an attribute only if the attribute
        isn't already taken. For example if you have a table named 'conn' it will never be stored
        as an attribute because that name is already reserved for the SQL connection,
        so in this case you will have to access it like a dictionary: db['conn']

        If a table is added to the Database after initializing the instance, only once the user
        tries to get it (from __getitem__ or __getattribute__) then it will be created
        and stored in the instance.

        If a requested table isn't present in the Database, KeyError is raised
        """
        # TODO: test tables added after __init__
        for table in self.db.tables:
            non_existent_table = f'{table} {0.32}'
            self.assertRaisesRegex(
                KeyError,
                f'No such Table: {non_existent_table}, must be one of the following: {", ".join(self.db.tables)}',
                self.db.__getitem__, non_existent_table
            )
        for table in self.db.tables:
            table_item = self.db[table]
            table_attr = getattr(self, table, None)

            self.assertIsInstance(table_item, Table)
            self.assertEqual(table_item.name, table)

            if table_attr is not None:
                self.assertIsInstance(table_attr, Table)
                self.assertEqual(table_attr.name, table)

                self.assertEqual(table_attr, table_item)
                self.assertEqual(id(table_attr), id(table_item))

    def test_len(self):
        out = len(self.db)
        self.assertIsInstance(out, int)
        self.assertEqual(out, len(self.db.tables))

    def test_repr(self):
        self.assertIsInstance(repr(self.db), str)
        self.assertIsInstance(str(self.db), str)
        self.assertEqual(repr(self.db), str(self.db))


if __name__ == '__main__':
    unittest.main()
