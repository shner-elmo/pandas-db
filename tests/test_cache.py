import unittest
import sqlite3

from pandasdb import Database
from pandasdb.cache import Cache, CacheDict


DB_FILE = '../data/forestation.db'

MIN_TABLES = 1
MIN_COLUMNS = 3  # for the first table


class TestCacheDict(unittest.TestCase):
    def setUp(self) -> None:
        self.cache = CacheDict()

    def test_init(self):
        self.assertTrue(isinstance(self.cache, CacheDict))
        self.assertTrue(isinstance(self.cache, dict))

    def test_setitem(self):
        types = [dict(), set(), tuple(), list(),  3, 3.32, None, True]
        for t in types:
            self.assertRaisesRegex(
                TypeError,
                f'Key must be of type str not {type(t)}',
                self.cache.__setitem__, t, [('jake',)]
            )
        
        types = [dict(), set(), tuple(), 3, 3.32, '3.32', None, True]
        for t in types:
            self.assertRaisesRegex(
                TypeError,
                f'Value must be of type list not {type(t)}',
                self.cache.__setitem__, 'SELECT ...', t
            )
        
        self.assertEqual(len(self.cache), 0)

        self.cache['SELECT MIN(a) FROM table'] = [(0.02, )]
        self.cache['SELECT MAX(a) FROM table'] = [(436.64, )]
        self.cache['SELECT AVG(a) FROM table'] = [(42.04, )]
        self.cache['SELECT LEN(x) FROM table'] = [(78, )]

        self.assertEqual(len(self.cache), 4)

    def test_str(self):
        for i in range(12):
            self.cache[str(i)] = [(i,)]

        size = len(self.cache)
        self.assertEqual(size, 12)
        str_repr = str(self.cache)
        self.assertIn(member=str(size), container=str_repr)


class TestCache(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = sqlite3.connect(DB_FILE)

        self.db = Database(DB_FILE)
        table = self.db.tables[0]
        column = self.db[table].columns[0]
        self.table = self.db[table]
        self.queries = [
            f'SELECT MIN({column}) FROM {table}',
            f'SELECT MAX({column}) FROM {table}',
            f'SELECT COUNT({column}) FROM {table}',
            f'SELECT COUNT(*) FROM {table} WHERE {column} IS NULL'
        ]

    def tearDown(self) -> None:
        self.conn.close()
        self.db.exit()

    def test_init(self):
        cache = Cache(conn=self.conn, cache_output=False)
        self.assertIsInstance(cache, CacheDict)
        self.assertIsInstance(cache, dict)
        self.assertEqual(len(cache), 0)

    def test_is_ready(self):
        cache = Cache(conn=self.conn, cache_output=False)
        out = cache.is_ready
        self.assertIsInstance(out, bool)
        self.assertIs(out, False)

        db = Database(DB_FILE, cache=True, populate_cache=False)
        self.assertFalse(db.cache.is_ready)
        db.exit()

        db = Database(DB_FILE, cache=True, populate_cache=True)
        self.assertTrue(db.cache.is_ready)
        db.exit()

    def test_execute(self):
        cache = Cache(conn=self.conn, cache_output=False)
        out = cache.execute(f'SELECT * FROM {self.table.name}')

        self.assertIsInstance(out, list)
        self.assertEqual(len(self.table), len(out))
        self.assertEqual(len(cache), 0)
        del out

        cache = Cache(conn=self.conn, cache_output=True)
        for query in self.queries:
            self.assertNotIn(member=query, container=cache)
            cache.execute(query)
            self.assertIn(member=query, container=cache)

        self.assertEqual(len(cache), len(self.queries))
        # TODO rewrite tests, max_dict_size is in MB not len()
        cache = Cache(
            conn=self.conn, 
            cache_output=True,
            max_item_size=0,
            max_dict_size=0
        )
        for q in self.queries:
            cache.execute(q)
        self.assertEqual(len(cache), 0)

        cache = Cache(
            conn=self.conn, 
            cache_output=True, 
            max_item_size=100,
            max_dict_size=0
        )
        for q in self.queries:
            cache.execute(q)
        self.assertEqual(len(cache), 0)

        cache = Cache(
            conn=self.conn, 
            cache_output=True, 
            max_item_size=0,
            max_dict_size=2
        )
        for q in self.queries:
            cache.execute(q)
        self.assertEqual(len(cache), 0)

        cache = Cache(
            conn=self.conn, 
            cache_output=True, 
            max_item_size=1,
            max_dict_size=2
        )
        for q in self.queries:
            cache.execute(q)
        self.assertEqual(len(cache), 4)

        cache = Cache(
            conn=self.conn, 
            cache_output=True, 
            max_item_size=1,
            max_dict_size=0.00034 * 2
        )
        for q in self.queries:
            cache.execute(q)
        self.assertEqual(len(cache), 2)

    def test_populate_table(self):
        db = Database(db_path=DB_FILE, cache=True, populate_cache=False)
        table_keys = [
            # "PRAGMA table_info('{table}')",  # already called in Table.items()
            'SELECT COUNT(*) FROM {table}'
        ]
        column_keys = [
            # '{query} WHERE {column} NOT NULL LIMIT 1',  # Column.median() now creates an SQL-view
            "PRAGMA table_info('{table}')",
            'SELECT COUNT(*) FROM {table}',
            'SELECT COUNT({column}) FROM {table}',
            'SELECT COUNT(*) FROM {table} WHERE {column} IS NULL',
            'SELECT MIN({column}) FROM {table}',
            'SELECT MAX({column}) FROM {table}',
        ]
        self.assertFalse(db.cache.is_ready)

        for table_name, table in db.items():
            for key in table_keys:
                key = key.format(table=table_name)
                self.assertNotIn(member=key, container=db.cache)

            db.cache.populate_table(table)

            for key in table_keys:
                key = key.format(table=table_name)
                self.assertIn(member=key, container=db.cache)

            for col_name, col in table.items():
                for key in column_keys:
                    key = key.format(column=col_name, table=table_name, query=col.query)
                    self.assertIn(member=key, container=db.cache)

        self.assertTrue(db.cache.is_ready)


if __name__ == '__main__':
    unittest.main()
