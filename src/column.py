from pandas import Series

import sqlite3
from typing import Generator

from .expression import Expression


class Column:
    """
    An object that represents a column of a table within a DataBase
    """
    def __init__(self, conn: sqlite3.Connection, table_name: str, col_name: str) -> None:
        self.conn = conn
        self._table = table_name
        self._name = col_name
        self.query = f'SELECT {col_name} FROM {table_name}'

    @property
    def type(self) -> str:
        """
        Get column type
        """
        with self.conn as cursor:
            for row in cursor.execute(f"PRAGMA table_info('{self._table}')"):
                if row[1] == self._name:
                    return row[2]

    @property
    def len(self) -> int:
        """
        Get the amount of rows/ cells in the column
        """
        return sum(1 for _ in self)

    def to_series(self) -> Series:
        """
        Return column as a Pandas Series
        """
        return Series(data=self, name=self._name)

    def data(self, limit: int = None) -> list:
        """
        Get column-data

        If limit is None: return all data, else: return n_amount of rows/ cells

        :param limit: int
        :return: list
        """
        with self.conn as cursor:
            if limit:
                return [x[0] for x in cursor.execute(self.query + f' LIMIT {limit}')]
            return [x[0] for x in cursor.execute(self.query)]

    def __iter__(self) -> Generator:
        """ Yield values from column """
        with self.conn as cursor:
            for i in cursor.execute(self.query):
                yield i[0]

    def __len__(self) -> int:
        """ Get amount of rows """
        return self.len

    def __hash__(self) -> int:
        """ Get hash value of Column """
        return hash(f'{self._table}.{self._name}')

    def __str__(self) -> str:
        """ Return column as a Pandas Series """
        return self.to_series().to_string(max_rows=10, index=True, name=True, length=True, dtype=True)

    def __repr__(self) -> str:
        """ Return column as a Pandas Series """
        return self.to_series().to_string(max_rows=10, index=True, name=True, length=True, dtype=True)

    # TODO: complete expressions docstrings
    def __gt__(self, other: float) -> Expression:
        """

        :param other: float
        :return: Expression
        """
        return Expression(query=f'{self._table}.{self._name} > {other} ')

    def __ge__(self, other: float) -> Expression:
        """

        :param other: float
        :return: Expression
        """
        return Expression(query=f'{self._table}.{self._name} >= {other} ')

    def __lt__(self, other: float) -> Expression:
        """

        :param other: float
        :return: Expression
        """
        return Expression(query=f'{self._table}.{self._name} < {other} ')

    def __le__(self, other: float) -> Expression:
        """

        :param other: float
        :return: Expression
        """
        return Expression(query=f'{self._table}.{self._name} <= {other} ')

    def __eq__(self, other: str | float) -> Expression:
        """

        :param other: str or float
        :return: Expression
        """
        if type(other) is str:
            return Expression(query=f"{self._table}.{self._name} = '{other}' ")
        return Expression(query=f'{self._table}.{self._name} = {other} ')

    def __ne__(self, other: str | float) -> Expression:
        """

        :param other: str or float
        :return: Expression
        """
        if type(other) is str:
            return Expression(query=f"{self._table}.{self._name} != '{other}' ")
        return Expression(query=f'{self._table}.{self._name} != {other} ')

    def isin(self, options: tuple) -> Expression:
        """

        :param options: tuple
        :return: Expression
        """
        if type(options) is not tuple:
            options = tuple(options)
        return Expression(query=f'{self._table}.{self._name} IN {options} ')

    def between(self, x: float, y: float) -> Expression:
        """

        :param x: float
        :param y: float
        :return: Expression
        """
        return Expression(query=f'{self._table}.{self._name} BETWEEN {x} AND {y} ')

    def like(self, regex: str) -> Expression:
        """

        :param regex: str
        :return: Expression
        """
        return Expression(query=f"{self._table}.{self._name} LIKE '{regex}' ")

    def ilike(self, regex: str) -> Expression:
        """

        :param regex: str
        :return: Expression
        """
        return Expression(query=f"{self._table}.{self._name} ILIKE '{regex}' ")
