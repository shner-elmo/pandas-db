from __future__ import annotations

from pandas import DataFrame

import sqlite3
import warnings
from typing import Generator, Any
from threading import Thread
from pathlib import Path

from .utils import convert_sql_to_db, rename_duplicate_cols, sqlite_conn_open
from .table import Table
from .exceptions import FileTypeError, InvalidTableError, ConnectionClosedWarning
from .cache import Cache


class Database:
    """
    A class that represents a database, all the tables will be stored as attributes in the Database object,
    and the columns will be stored as attributes in they're respective tables
    You can have a look at the README here: https://github.com/shner-elmo/pandas-db/blob/master/README.md
    """
    def __init__(self, db_path: str, cache: bool = True, populate_cache: bool = True, max_item_size: int = 2,
                 max_dict_size: int = 100, block_till_ready: bool = False) -> None:
        """
        Initialize the Database object

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
        path = Path(db_path)
        self.db_path = db_path

        extension = path.suffix
        valid_extension = ('.sql', '.db', '.sqlite', '.sqlite3')

        if extension not in valid_extension:
            raise FileTypeError(f'File extension must be one of the following: {", ".join(valid_extension)}')

        if extension == '.sql':  # if .sql load to .db file and then connect
            current_file_parts = Path(__file__).parent.parent.parts
            folder = Path(*current_file_parts, 'pandasdb-local-databases')
            file = Path(*folder.parts, path.stem + '.db')

            if not folder.exists():
                folder.mkdir()

            files = [f.name for f in folder.iterdir()]
            if file.name in files:
                file.unlink()  # delete file and create a new one to update the data

            convert_sql_to_db(sql_file=db_path, db_file=str(file))
            self.conn = sqlite3.connect(str(file), check_same_thread=False)
        else:
            self.conn = sqlite3.connect(db_path, check_same_thread=False)

        self.cache = Cache(
            conn=self.conn,
            cache_output=cache,
            max_item_size=max_item_size,
            max_dict_size=max_dict_size
        )

        self._table_items: dict[str, Table] = {}
        for table in self.tables:
            self._set_table(table=table)

        if cache and populate_cache:
            threads = []
            for _, table in self.items():
                thread = Thread(target=self.cache.populate_table, kwargs={'table': table})
                thread.start()
                threads.append(thread)

            if block_till_ready:
                for thread in threads:
                    thread.join()

    @property
    def tables(self) -> list[str]:
        """
        Get list of tables

        :return: list with table names
        """
        # avoid cache for tables added after
        with self.conn as cursor:
            return [x[0] for x in cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")]

    @property
    def views(self) -> list[str]:
        """
        Get a list of all views

        Note that the filter method creates custom views for its filters but those all start with a double underscores.

        :return: list with table names
        """
        # avoid cache for tables added after
        with self.conn as cursor:
            return [x[0] for x in cursor.execute("SELECT name FROM sqlite_master WHERE type='view'")]

    def drop_view(self, *tables: str) -> None:
        """
        Drop SQL Views

        :param tables: args, str
        :return: None
        """
        for table in tables:
            with self.conn as cursor:
                cursor.execute(f'DROP VIEW {table}')

    def get_columns(self, table_name: str) -> list[str]:
        """
        Get list of all columns within given table

        :param table_name: str
        :return: list with column names
        """
        if table_name not in self.tables + self.views:
            raise InvalidTableError(f'No such table: {table_name}')

        return [x[1] for x in self.cache.execute(f"PRAGMA table_info('{table_name}')")]

    def items(self) -> Generator[tuple[str, Table], None, None]:
        """
        Generator that yields: (table_name, table_object)
        """
        yield from self._table_items.items()

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

        if rename_duplicates:
            if len(set(cols)) != len(cols):
                cols = rename_duplicate_cols(cols)

        return DataFrame(data=data, columns=cols)

    def __enter__(self) -> 'Database':
        """
        Return the instance of Database

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

    def __del__(self) -> None:
        """
        Perform cleanup as soon as the object is deleted

        :return: None
        """
        if hasattr(self, 'conn') and sqlite_conn_open(self.conn):
            self.exit()

    def exit(self) -> None:
        """
        Close Database connection, this should be called when you're done using the connection

        :return: None
        """
        if sqlite_conn_open(self.conn):
            self.drop_view(*self.cache.views)
            self.conn.close()
        else:
            warnings.warn('Connection already closed!', ConnectionClosedWarning)

    def _set_table(self, table: str) -> None:
        """
        Create and set table-object to instance

        Create table object and save it to the self._table_items dictionary,
        and if the table-name isn't an existing attribute or method; then set it as an attribute as well.
        The table objects can be accessed using __getitem__ (db[table_name]) or as an attribute (db.table_name).

        :param table: str
        :return: None
        """
        table_obj = Table(conn=self.conn, cache=self.cache, name=table)
        self._table_items[table] = table_obj

        if not hasattr(self, table):  # to avoid overwriting existing attributes and methods
            setattr(self, table, table_obj)

    def __getitem__(self, table: str) -> Table:
        """
        Get Table object for given table_name

        :param table: str, table name
        :raise: KeyError if key not found
        :return: Table
        """
        if table not in self.tables:
            raise KeyError(f'No such Table: {table}, must be one of the following: {", ".join(self.tables)}')

        if table not in self._table_items:  # if table was created after the instance:
            self._set_table(table=table)

        return self._table_items[table]

    def __getattribute__(self, item) -> Any:
        """ Get attribute """
        # for avoiding 'Unresolved attribute' warnings (in Pycharm), this somehow fixes it
        return super().__getattribute__(item)

    def __len__(self) -> int:
        """ Get the number of tables in the database """
        return len(self.tables)

    def __repr__(self) -> str:
        """ Get the string representation of the class instance """
        return __class__.__name__ + "(db_path='{}')".format(self.db_path)
