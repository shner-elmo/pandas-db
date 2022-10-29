import unittest

from pandasdb.utils import *

DB_FILE = '../data/forestation.db'
SQL_FILE = '../data/parch-and-posey.sql'
SQLITE_FILE = '../data/mental_health.sqlite'


class TestUtils(unittest.TestCase):
    def test_convert_type_to_sql(self):
        self.assertEqual(convert_type_to_sql('jake snake'), "'jake snake'")
        self.assertEqual(convert_type_to_sql(394), '394')
        self.assertEqual(convert_type_to_sql(42.43), '42.43')
        self.assertEqual(convert_type_to_sql(True), 'true')
        self.assertEqual(convert_type_to_sql(False), 'false')

    def test_sql_tuple(self):
        out = sql_tuple(('jake', 32.2, True, 'new york'))
        self.assertEqual(out, "('jake', 32.2, true, 'new york')")

        out = sql_tuple((False,))
        self.assertEqual(out, '(false)')

    def test_sqlite_conn_open(self):
        conn = sqlite3.connect(DB_FILE)
        # with conn as cur:
        #     cur.execute('SELECT ')
        conn.cursor()  # if connection is closed it will raise an error when asked for a cursor
        self.assertIs(sqlite_conn_open(conn), True)
        conn.close()
        self.assertIs(sqlite_conn_open(conn), False)

        self.assertRaises(sqlite3.ProgrammingError, conn.cursor)

    def test_get_random_name(self):
        out = get_random_name(5)
        self.assertIsInstance(out, str)
        self.assertTrue(out.islower())
        self.assertEqual(len(out), 5)

        # assert set doesn't shrink in size (all elements unique)
        random_names = {
            get_random_name(), get_random_name(),
            get_random_name(), get_random_name(),
            get_random_name(), get_random_name()
        }
        self.assertEqual(len(random_names), 6)

    def test_create_view(self):
        def get_views() -> list:
            with conn as cursor:
                return [x[0] for x in cursor.execute("SELECT name FROM sqlite_master WHERE type='view'")]

        conn = sqlite3.connect(DB_FILE)
        name = f'test_view_{get_random_name()}'
        query = 'SELECT * FROM forest_area LIMIT 50'

        self.assertNotIn(member=name, container=get_views())
        create_view(conn=conn, view_name=name, query=query, drop_if_exists=True)
        self.assertIn(member=name, container=get_views())

        with conn as cur:
            view_data: list[tuple[Any]] = cur.execute(f'SELECT * FROM {name}').fetchall()
            table_data: list[tuple[Any]] = cur.execute(query).fetchall()
        self.assertEqual(view_data, table_data)

        self.assertRaisesRegex(
            ValueError,
            f"view '{name}' already exists",
            create_view, conn=conn, view_name=name, query=query, drop_if_exists=False
        )

        create_view(conn=conn, view_name=name, query=query, drop_if_exists=True)
        self.assertIn(member=name, container=get_views())

        with conn as cur:  # cleanup after test
            cur.execute(f'DROP VIEW {name}')

    def test_concat(self):
        first = ['jake', 'carla', 'francis', 'john']
        last = ['snake', 'louie', 'ngannou', 'cash']
        out = ['jake snake',  'carla louie', 'francis ngannou', 'john cash']

        # make sure each element is as expected, and both iterables have the same length (with strict=True)
        it = concat(first, ' ', last)
        for a, b in zip(it, out, strict=True):
            self.assertIsInstance(a, str)
            self.assertEqual(a, b)

        it = concat(first, last, sep=' ')
        for a, b in zip(it, out, strict=True):
            self.assertEqual(a, b)

        ages = [32, 19, 30, 53]
        out = ['jake snake - 32',  'carla louie - 19', 'francis ngannou - 30', 'john cash - 53']
        it = concat(first, ' ', last, ' - ', ages)
        for a, b in zip(it, out, strict=True):
            self.assertEqual(a, b)

        out = ['jake snake 32',  'carla louie 19', 'francis ngannou 30', 'john cash 53']
        it = concat(first, last, ages, sep=' ')
        for a, b in zip(it, out, strict=True):
            self.assertEqual(a, b)

    def test_mb_size(self):
        pass

    def test_rename_duplicate_cols(self):
        cols = ['a', 'b', 'c']
        out = rename_duplicate_cols(cols)
        self.assertEqual(len(cols), len(out))

        cols = ['a', 'b', 'c', 'a', 'b', 'b']
        out = rename_duplicate_cols(cols)
        unique_out: set[str] = set(out)
        self.assertEqual(len(cols), len(unique_out))
        self.assertEqual(out, ['a', 'b', 'c', 'a_2', 'b_2', 'b_3'])

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
