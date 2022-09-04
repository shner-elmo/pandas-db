from pandas import Series

import sqlite3
from typing import Generator, Callable

from .expression import Expression
from .indexloc import IndexLoc


class Column:
    """
    An object that represents a column of a table within a DataBase
    """
    def __init__(self, conn: sqlite3.Connection, table_name: str, col_name: str) -> None:
        self.conn = conn
        self._table = table_name
        self._name = col_name
        self._query = f'SELECT {col_name} FROM {table_name}'

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
        with self.conn as cursor:
            return cursor.execute(f'SELECT COUNT(*) FROM {self._table}').fetchone()[0]

    def to_series(self) -> Series:
        """
        Return column as a Pandas Series
        """
        return Series(data=iter(self), name=self._name)

    def data(self, limit: int = None) -> list:
        """
        Get column-data

        If limit is None: return all data, else: return n_amount of rows/ cells

        :param limit: int
        :return: list
        """
        with self.conn as cursor:
            if limit:
                return [x[0] for x in cursor.execute(self._query + f' LIMIT {limit}')]
            return [x[0] for x in cursor.execute(self._query)]

    def apply(self, func: Callable, *, ignore_na: bool = False) -> Generator:
        """
        Apply function on each cell in the column

        example:
        db = DataBase('data/parch-and-posey.sql')
        column = db.accounts.primary_poc.apply(lambda x: x.split()[0])
        for first_name in column:
            print(first_name)

        'Tamara'
        'Sung'
        'Jodee'
        'Serafina'

        :param func: Callable
        :param ignore_na: bool, default: False
        :return: Generator
        """
        for cell in self:
            if cell is None and ignore_na:
                yield cell
            else:
                yield func(cell)

    @property
    def iloc(self) -> IndexLoc:
        """
        Get data by: index, list, or slice

        Getitem supports three ways of indexing the iterable:
        1) Singular Integer, ex: IndexIloc[0], IndexIloc[32], or with negative: IndexIloc[-12]
        2) Passing a list of integers, ex: IndexIloc[[1, 22, 4, 3, 17, 38]], IndexIloc[[1, -4, 17, 22, 38, -4, -1]]
        4) Passing Slice, ex: IndexIloc[:10], IndexIloc[2:8], IndexIloc[2:24:2]

        The return type will be a list for multiple items,
        and one of the following: str, int, or float. Depending on the data type of the column

        :return: list, str, int, or float
        """
        return IndexLoc(it=iter(self), length=len(self))

    def __getitem__(self, item: int | slice | list):  # -> list | str | int | float
        """ Return index slice """
        return self.iloc[item]

    def __iter__(self) -> Generator:
        """ Yield values from column """
        with self.conn as cursor:
            for i in cursor.execute(self._query):
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
