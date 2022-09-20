import sqlite3

from .utils import mb_size


class Cache:
    """
    A class for managing the cache for all the SQL queries
    """
    def __init__(self, conn: sqlite3.Connection, max_item_size: int = 2, max_dict_size: int = 100) -> None:
        """
        Initialize the cache

        :param conn: Sqlite3 Connection
        :param max_item_size: int, max cache item size in MB (key + value)
        :param max_dict_size: int, max cache dict size in MB
        """
        self.conn = conn
        self.max_item_size = max_item_size
        self.max_dict_size = max_dict_size

        self.data: dict[str, list] = {}
        self.size = 0
        self._ready_count = 0

    @property
    def is_ready(self) -> bool:
        """
        Return true if cache is populated with all tables
        """
        with self.conn as cursor:
            tables = cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")

        return self._ready_count == len(list(tables))

    def add_cache(self, key: str, val: list) -> None:
        """
        Add key and value to cache

        Set the SQL query as the key, and the output as the value.
        If the

        :param key:
        :param val:
        :raise ValueError: if not isinstance(key, str) or not isinstance(val, str)
        :return:
        """
        if not isinstance(key, str):
            raise ValueError(f'key must be of type str not {type(key)}')
        if not isinstance(val, list):
            raise ValueError(f'value must be of type list not {type(key)}')

        self.data[key] = val

    def reset_cache(self) -> None:
        """
        Reset cache dictionary

        :return: None
        """
        self.data.clear()

    def execute(self, query: str) -> list:
        """
        Execute an SQL query and save the result in cache for next time

        :param query: str
        :return: list with query results
        """
        if query in self.data:
            return self.data[query]

        with self.conn as cursor:
            query_out = cursor.execute(query).fetchall()

        size = mb_size(query, query_out)

        if size <= self.max_item_size and size + self.size <= self.max_dict_size:
            self.add_cache(key=query, val=query_out)
            self.size += size
        else:
            print(f'item refused: \n {size=}, {query=}')

        return query_out

    def populate_table(self, table: 'Table') -> None:
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

        print(f'cached ready for {table._name}')
        self._ready_count += 1
