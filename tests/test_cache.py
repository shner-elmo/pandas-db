# import unittest
# import sqlite3
#
# from pandasdb.table import Table
# from pandasdb.column import Column
# from pandasdb.cache import Cache
#
#
# DB_FILE = '../data/forestation.db'
#
# MIN_TABLES = 1
# MIN_COLUMNS = 3  # for the first table
#
#
# class TestCache(unittest.TestCase):
#     def setUp(self) -> None:
#         self.conn = sqlite3.connect(DB_FILE, block_till_ready=True)
#         self.cache = Cache(conn=self.conn, max_cache_size=None, max_cache_item_size=None)
#
#     def tearDown(self) -> None:
#         self.conn.close()
#
#     def test_init(self):
#         self.assertIsInstance(self.cache.data, dict)
#         self.assertEqual(len(self.cache.data), 0)
#
#     def test_optional_args(self):
#         pass  # TODO test max_cache_size and max_cache_item_size
#
#     def test_add_cache(self):
#         cache_dict = self.cache.data
#         self.assertEqual(len(cache_dict), 0)
#
#         key = 'SELECT * FROM table'
#         val = [('a', 1, False), ('d', 0, True)]
#         self.cache.add_cache(key=key, val=val)
#
#     def test_reset_cache(self):
#         pass
#
#     def test_execute(self):
#         pass
#
#     def test_mb_size(self):
#         pass
#
#
# if __name__ == '__main__':
#     unittest.main()
#
# """ In DataBase.__init__() the cache-dict is filled with the table and column data (name, type, etc..)"""
#
# # cache = self.db.cache.data
# # self.assertIsInstance(cache, dict)
# # self.assertEqual(len(cache), len(self.db.tables))
# #
# # for key, val in cache.items():
# #     self.assertIsInstance(key, str)
# #     self.assertIsInstance(val, list)
# # move to test_DataBase -> test_init
