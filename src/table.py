from pandas import DataFrame

import sqlite3
from typing import Generator

from .exceptions import InvalidColumnError
from .column import Column


class Table:
    """
    An object that represents an SQL table
    """
    def __init__(self, conn: sqlite3.Connection, name: str) -> None:
        self.conn = conn
        self._name = name
        self.query = f'SELECT * FROM {self._name}'

        for col in self.columns:
            setattr(self, col, Column(conn=self.conn, table_name=self._name, col_name=col))

    @property
    def columns(self) -> list[str]:
        """
        Get list with column names
        """
        with self.conn as cursor:
            return [x[1] for x in cursor.execute(f"PRAGMA table_info('{self._name}')")]

    @property
    def len(self) -> int:
        """
        Return amount of rows in the table
        """
        return sum(1 for _ in self)

    @property
    def shape(self) -> tuple:
        """
        Get a tuple with: (n_rows, n_cols)
        """
        return self.len, len(next(iter(self)))

    def to_df(self) -> DataFrame:
        """
        Return table as a Pandas DataFrame
        """
        return DataFrame(data=self, columns=self.columns)

    def data(self, limit: int = None) -> list:
        """
        Get table data in a nested list, ex: [('AMD', 78.54, True), ('AAPL', 125.34, True)...]

        :param limit: int
        :return: list
        """
        with self.conn as cursor:
            if limit:
                return cursor.execute(self.query + f' LIMIT {limit}').fetchall()
            return cursor.execute(self.query).fetchall()

    def items(self) -> Generator:
        """
        Generator that yields: (column_name, col_object)
        """
        for col in self.columns:
            yield col, getattr(self, col)

    def __iter__(self) -> Generator:
        """
        Yield rows from cursor
        """
        with self.conn as cursor:
            yield from cursor.execute(self.query)

    def _get_col(self, column: str) -> Column:
        """

        :param column: column-name
        :return: Column
        :raise: InvalidColumnError
        """
        if column not in self.columns:
            raise InvalidColumnError(f'Column must be one of the following: {", ".join(self.columns)}')
        return getattr(self, column)

    def __getitem__(self, item: str) -> Column:
        """
        Get column object for given column name

        :param item: str, column-name
        :return: Column
        :raise: KeyError
        """
        try:
            return self._get_col(item)
        except InvalidColumnError:
            raise KeyError

    def __getattr__(self, item: str) -> Column:
        """
        Get column object for given column name

        :param item: str, column-name
        :return: Column
        :raise: AttributeError
        """
        try:
            return self._get_col(item)
        except InvalidColumnError:
            raise AttributeError

    def __len__(self) -> int:
        """ Return amount of rows """
        return self.len

    def __hash__(self) -> int:
        """ Get hash value of Table """
        return hash(f'{self._name}')

    def __str__(self) -> str:
        """ Return table as a Pandas DataFrame """
        return self.to_df().to_string(max_rows=10, max_cols=10, show_dimensions=True)

    def __repr__(self) -> str:
        """ Return table as a Pandas DataFrame """
        return self.to_df().to_string(max_rows=10, max_cols=10, show_dimensions=True)
