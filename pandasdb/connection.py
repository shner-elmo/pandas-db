from pandas import DataFrame

import sqlite3
import warnings
from typing import Generator
from threading import Thread

from .utils import load_sql_to_sqlite, rename_duplicate_cols
from .table import Table
from .exceptions import FileTypeError, InvalidTableError
from .cache import Cache


class DataBase:
    """
    A class that represents a database, all the tables will be stored as attributes in the DataBase object,
    and the columns will be stored as attributes in they're respective tables
    You can have a look at the README here: https://github.com/shner-elmo/pandas-db/blob/master/README.md
    """
    def __init__(self, db_path: str, cache: bool = True, populate_cache: bool = True, max_item_size: int = 2,
                 max_dict_size: int = 100, block_till_ready: bool = False) -> None:
        """
        Initialize the DataBase object

        The only required parameter is db_path, which takes a database file of type: db, sql, or sqlite.

        There are optional parameters for managing the cache, whenever the user calls a method like: Column.min()
        the method will check if we already stored the output of the SQL query ("SELECT MIN(Column) FROM Table")
        for the method in the cache, if we did, then it will return the result directly from the cache,
        otherwise: run the SQL query and save the output in cache for next time.

        The optional parameters for managing the cache are:

        cache: if True it will store the results of each SQL query in a dictionary for next time

        populate_cache: if True it will call the methods for each column in each table in the database,
        when its initialized, this way when the user calls the method it will already be present in cache
        and ready to use.

        max_item_size: the max size in Megabytes an item can take in cache (sql_query + query_output)

        max_dict_size: the max size of the whole cache-dictionary, if the dictionary reaches its max size,
        no item will be added.

        block_till_ready: by default the Database will populate the cache in the background,
        so after the initialization the user can use the Database and in the background the cache will start
        getting filled, you can set this to False which will stop the user from executing any code until the
        cache is full.

        :param db_path: str, path to database
        :param cache: bool, default True
        :param populate_cache: bool, default True
        :param max_item_size: int, size in MB
        :param max_dict_size: int, size in MB
        :param block_till_ready: bool, default False
        """
        self.db_path = db_path
        extension = db_path.split('.')[-1]
        valid_extension = ('sql', 'db', 'sqlite', 'sqlite3')

        if extension not in valid_extension:
            raise FileTypeError(f'File extension must be one of the following: {valid_extension}')

        if extension == 'sql':
            self.conn = load_sql_to_sqlite(db_path)
        else:
            self.conn = sqlite3.connect(db_path, check_same_thread=False)

        self.cache = Cache(
            conn=self.conn,
            cache_output=cache,
            max_item_size=max_item_size,
            max_dict_size=max_dict_size
        )

        for table in self.tables:
            setattr(self, table, Table(conn=self.conn, cache=self.cache, name=table))

        if cache and populate_cache:
            threads = []
            for _, table in self.items():
                thread = Thread(target=self.cache.populate_table, kwargs={'table': table})
                thread.start()
                threads.append(thread)

            if block_till_ready:
                for thread in threads:
                    thread.join()

    def exit(self) -> None:
        """
        Close DataBase connection, this should be called when you're done using the connection

        :return: None
        """
        try:
            self.conn.close()
        except sqlite3.ProgrammingError:
            warnings.warn('Connection already closed!')

    @property
    def tables(self) -> list[str]:
        """
        Get list of tables

        :return: list with table names
        """
        return [x[0] for x in self.cache.execute("SELECT name FROM sqlite_master WHERE type='table'")]

    def get_columns(self, table_name: str) -> list[str]:
        """
        Get list of all columns within given table

        :param table_name: str
        :return: list with column names
        """
        if table_name not in self.tables:
            raise InvalidTableError(f'No such table: {table_name}')

        return [x[1] for x in self.cache.execute(f"PRAGMA table_info('{table_name}')")]

    def items(self) -> Generator[tuple[str, Table], None, None]:
        """
        Generator that yields: (table_name, table_object)
        """
        for table in self.tables:
            yield table, getattr(self, table)

    def query(self, sql_query: str, rename_duplicates: bool = True) -> DataFrame:
        """
        Return a DataFrame with the query results

        Returns a DataFrame with the results of a given query,
        if there are columns with duplicated names it will add a number at the end, for ex:
        ['a', 'b', 'c', 'a', 'b', 'b'] -> ['a', 'b', 'c', 'a_2', 'b_2', 'b_3']

        :param sql_query: str, SQL query
        :param rename_duplicates: bool, default: True
        :return: DataFrame
        """
        with self.conn as cursor:
            data = cursor.execute(sql_query)

        cols = [x[0] for x in data.description]

        duplicates = len(set(cols)) != len(cols)
        if duplicates and rename_duplicates:
            cols = rename_duplicate_cols(cols)

        return DataFrame(data=data, columns=cols)

    def __enter__(self) -> 'DataBase':
        """
        Return the instance of DataBase

        :return: self
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        Safely close the Database connection

        :param exc_type:
        :param exc_val:
        :param exc_tb:
        :return: None
        """
        self.exit()

    def _get_table(self, table_name: str) -> Table:
        """
        Get Table object for given table_name

        :param table_name: str
        :raise: ValueError if table name is not valid
        :return: Table
        """
        if table_name not in self.tables:
            raise InvalidTableError(f'No such table: {table_name}')

        if not hasattr(self, table_name):  # if table was created after the instance:
            setattr(self, table_name, Table(conn=self.conn, cache=self.cache, name=table_name))

        return getattr(self, table_name)

    def __getitem__(self, item: str) -> Table:
        """
        Get Table object for given table_name

        :param item: str, table name
        :raise: KeyError if key not found
        :return: Table
        """
        try:
            return self._get_table(item)
        except InvalidTableError:
            raise KeyError(f'No such Table: {item}, must be one of the following: {", ".join(self.tables)}')

    def __getattr__(self, attr: str) -> Table:
        """
        Get Table object for given table_name

        :param attr:
        :raise: AttributeError if attribute not found
        :return: Table
        """
        try:
            return self._get_table(attr)
        except InvalidTableError:
            raise AttributeError(f'No such attribute: {attr}')

    def __len__(self) -> int:
        """ Get the number of tables in the database """
        return len(self.tables)

    def __str__(self) -> str:
        """ Get the string representation of the class instance """
        return __class__.__name__ + "(db_path='{}')".format(self.db_path)

    def __repr__(self) -> str:
        """ Get the string representation of the class instance """
        return __class__.__name__ + "(db_path='{}')".format(self.db_path)
