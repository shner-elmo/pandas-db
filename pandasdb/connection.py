from pandas import DataFrame

import sqlite3
import warnings

from .utils import load_sql_to_sqlite
from .table import Table
from .exceptions import FileTypeError, InvalidTableError


class DataBase:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        extension = db_path.split('.')[-1]
        valid_extension = ('sql', 'db', 'sqlite', 'sqlite3')

        if extension not in valid_extension:
            raise FileTypeError(f'File extension must be one of the following: {valid_extension}')

        if extension == 'sql':
            self.conn = load_sql_to_sqlite(db_path)
        else:
            self.conn = sqlite3.connect(db_path)

        for table in self.tables:
            setattr(self, table, Table(conn=self.conn, name=table))

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
        with self.conn as cursor:
            return [x[0] for x in cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")]

    def get_columns(self, table_name: str) -> list[str]:
        """
        Get list of all columns within given table

        :param table_name: str
        :return: list with column names
        """
        if table_name not in self.tables:
            raise InvalidTableError(f'No such table: {table_name}')

        with self.conn as cursor:
            return [x[1] for x in cursor.execute(f"PRAGMA table_info('{table_name}')")]

    @staticmethod
    def _rename_duplicate_cols(columns: list) -> list:
        """
        for each duplicated column it will add a number as the suffix

        ex: ['a', 'b', 'c',  'a', 'b', 'b'] -> ['a', 'b', 'c',  'a_2', 'b_2', 'b_3']

        :param columns: DataFrame
        :return: list
        """
        new_cols = []
        prev_cols = []  # previously iterated columns in for loop

        for col in columns:
            prev_cols.append(col)
            count = prev_cols.count(col)

            if count > 1:
                new_cols.append(f'{col}_{count}')
            else:
                new_cols.append(col)
        return new_cols

    def query(self, sql_query: str, rename_duplicates: bool = True) -> DataFrame:
        """
        Return a DataFrame with the query results

        Returns a DatFrame with the results of a given query,
        if there are columns with duplicated names it will add a number at the end, for ex:
        ['a', 'b', 'c',  'a', 'b', 'b'] -> ['a', 'b', 'c',  'a_2', 'b_2', 'b_3']

        :param sql_query: str, SQL query
        :param rename_duplicates: bool, default: True
        :return: DataFrame
        """
        with self.conn as cursor:
            data = cursor.execute(sql_query)

        cols = [x[0] for x in data.description]

        duplicates = len(set(cols)) != len(cols)
        if duplicates and rename_duplicates:
            cols = self._rename_duplicate_cols(cols)

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
            setattr(self, table_name, Table(conn=self.conn, name=table_name))

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
            raise KeyError(f'No such Table: {item}, must be one of the following: {", ".join(self.table)}')

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

    def __str__(self) -> str:
        """ Get the string representation of the class instance """
        return __class__.__name__ + "(db_path='{}')".format(self.db_path)

    def __repr__(self) -> str:
        """ Get the string representation of the class instance """
        return __class__.__name__ + "(db_path='{}')".format(self.db_path)
