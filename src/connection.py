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
        Close DataBase connection, this method should be called before closing the app

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

    def __enter__(self) -> 'DataBase':
        """
        Return the instance of DataBase

        :return: self
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """

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
            raise KeyError

    def __getattr__(self, item: str) -> Table:
        """

        :param item:
        :raise: AttributeError if attribute not found
        :return: Table
        """
        try:
            return self._get_table(item)
        except InvalidTableError:
            raise AttributeError

    def __str__(self) -> str:
        """ Get the string representation of the class instance """
        return __class__.__name__ + "(db_path='{}')".format(self.db_path)

    def __repr__(self) -> str:
        """ Get the string representation of the class instance """
        return __class__.__name__ + "(db_path='{}')".format(self.db_path)
