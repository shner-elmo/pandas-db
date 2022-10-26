from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING

from .utils import get_mb_size
if TYPE_CHECKING:
    from .table import Table


class CacheDict(dict):
    def __setitem__(self, key: str, value: list[tuple]) -> None:
        """
        Add key and value to cache

        Set the SQL query as the key, and the output as the value.

        :param key: str, SQL query
        :param value: list, list with query output (Cursor.fetchall())
        :raise TypeError: if not isinstance(key, str) or not isinstance(val, str)
        :return: None
        """
        if not isinstance(key, str):
            raise TypeError(f'Key must be of type str not {type(key)}')
        if not isinstance(value, list):
            raise TypeError(f'Value must be of type list not {type(value)}')

        super().__setitem__(key, value)

    def __str__(self) -> str:
        """ Get amount of items in the dictionary """
        return f'Cache items: {len(self)}'

    def __repr__(self) -> str:
        """ Get representation of dictionary """
        return super().__repr__()


class Cache(CacheDict):
    """
    A class for managing the cache for all the SQL queries
    """
    def __init__(self, conn: sqlite3.Connection, cache_output: bool, max_item_size: float = 2.0,
                 max_dict_size: float = 100.0) -> None:
        """
        Initialize the cache

        :param conn: Sqlite3 Connection
        :param cache_output: bool, Cache output of SQL queries ?
        :param max_item_size: float, max cache item size in MB (key + value)
        :param max_dict_size: float, max cache dict size in MB
        """
        super().__init__()
        self.conn = conn
        self.cache_output = cache_output
        self.max_item_size = max_item_size
        self.max_dict_size = max_dict_size

        self.mb_size = 0
        self._ready_count = 0
        # list of SQL views created after initialization (they will all get dropped in `connection.Database.exit()`)
        self.views: list[str] = []

    @property
    def is_ready(self) -> bool:
        """
        Return true if cache is populated with all tables
        """
        tables = [x[0] for x in self.execute("SELECT name FROM sqlite_master WHERE type='table'")]
        return self._ready_count == len(tables)

    def execute(self, query: str) -> list:
        """
        Execute an SQL query and save the result in cache for next time

        :param query: str
        :return: list with query results
        """
        if not self.cache_output:
            with self.conn as cursor:
                return cursor.execute(query).fetchall()

        if query in self:
            return self[query]

        with self.conn as cursor:
            query_out = cursor.execute(query).fetchall()

        out_size = get_mb_size(query, query_out)
        if out_size <= self.max_item_size and out_size + self.mb_size <= self.max_dict_size:
            self[query] = query_out
            self.mb_size += out_size

        return query_out

    def populate_table(self, table: Table) -> None:
        """
        Call the most common methods for each column in the table to start populating the cache-dict

        :param table: Table, table object
        :return: None
        """
        getattr(table, 'len')
        getattr(table, 'columns')

        for _, col in table.items():
            getattr(col, 'type')
            getattr(col, 'sql_type')
            getattr(col, 'len')

            col.count(),
            col.na_count(),
            col.min(),
            col.max(),
            col.describe(),

            if col.data_is_numeric():
                col.sum(),
                col.avg(),
                col.median(),

            if col.type in (str, int) and len(table) < 1_000_000:
                col.mode(),
                col.unique(),
                col.value_counts()

        self._ready_count += 1
