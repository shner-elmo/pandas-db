from src import DataBase
from src.table import Table
from src.exceptions import InvalidTableError

import unittest
import sqlite3

DB_FILE = '../data/forestation.db'
SQL_FILE = '../data/parch-and-posey.sql'
SQLITE_FILE = '../data/mental_health.sqlite'
MAIN_DATABASE = DB_FILE

MIN_TABLES = 2


class TestConnection(unittest.TestCase):
    def setUp(self):
        self.db = DataBase(MAIN_DATABASE)

        tables = self.db.tables
        self.assertGreaterEqual(len(tables), MIN_TABLES,
                                msg='Data Base must have at least one table for the tests')

    def tearDown(self):
        self.db.exit()

    # test Extensions (.db, .sql, .sqlite)
    def test_file_type_db(self):
        db = DataBase(DB_FILE)
        self.assertListEqual(db.tables, ['forest_area', 'land_area', 'regions'])
        db.exit()

    def test_file_type_sql(self):
        db = DataBase(SQL_FILE)
        self.assertListEqual(db.tables, ['web_events', 'sales_reps', 'region', 'orders', 'accounts'])
        db.exit()

    def test_file_type_sqlite(self):
        db = DataBase(SQLITE_FILE)
        self.assertListEqual(db.tables, ['Answer', 'Question', 'Survey'])
        db.exit()

    def test_exit(self):
        db = DataBase(MAIN_DATABASE)
        table = db.tables[0]
        db.exit()

        self.assertRaisesRegex(
            sqlite3.ProgrammingError,
            '^Cannot operate on a closed cursor.$',
            db.get_columns, table
        )

    def test_tables(self):
        out = self.db.tables
        self.assertEqual(type(out), list)
        self.assertGreaterEqual(len(out), MIN_TABLES)

    def test_get_columns(self):
        out = self.db.get_columns(self.db.tables[0])
        self.assertEqual(type(out), list)
        self.assertGreaterEqual(len(out), MIN_TABLES)

    def test_context_manager(self):
        with DataBase(MAIN_DATABASE) as data_base:
            table = data_base.tables[0]
            self.assertIsInstance(data_base, DataBase)

        self.assertRaisesRegex(
            sqlite3.ProgrammingError,
            '^Cannot operate on a closed cursor.$',
            data_base.get_columns, table
        )

    def test_get_table(self):
        # add test for tables created after init
        """
        DataBase.__getitem__() and DataBase.__getattr__()
        are two different ways to get the table object,
        they both call DataBase._get_table() to get the object which is stored
        as an attribute, and consequently they both return the same preexisting
        Table object (set in __init__()).

        If a table is added to the DataBase after initializing the instance
        it will create the Table object.

        If a requested table isn't present in the Database, InvalidTableError is raised
        """
        name = self.db.tables[0]

        table_attr = getattr(self.db, name)
        self.assertIsInstance(table_attr, Table)

        table_item = self.db[name]
        self.assertIsInstance(table_item, Table)

        self.assertEqual(table_attr, table_item)

        # assert it dosent raises Exception
        self.db._get_table(name)

        non_existing_table = 'Hello There'
        self.assertRaisesRegex(
            InvalidTableError,
            f'^No such table: {non_existing_table}$',
            self.db._get_table, non_existing_table
        )

    def test_repr(self):
        self.assertIsInstance(repr(self.db), str)
        self.assertIsInstance(str(self.db), str)


if __name__ == '__main__':
    unittest.main()
