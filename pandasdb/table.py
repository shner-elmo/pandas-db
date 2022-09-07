from pandas import DataFrame

import sqlite3
from typing import Generator, Callable

from .exceptions import InvalidColumnError
from .column import Column
from .indexloc import IndexLoc


class Table:
    """
    An object that represents an SQL table
    """
    def __init__(self, conn: sqlite3.Connection, name: str) -> None:
        self.conn = conn
        self._name = name
        self._query = f'SELECT * FROM {self._name}'

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
        with self.conn as cursor:
            return cursor.execute(f'SELECT COUNT(*) FROM {self._name}').fetchone()[0]

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
        return DataFrame(data=iter(self), columns=self.columns)

    def data(self, limit: int = None) -> list:
        """
        Get table data in a nested list, ex: [('AMD', 78.54, True), ('AAPL', 125.34, True)...]

        :param limit: int
        :return: list
        """
        with self.conn as cursor:
            if limit:
                return cursor.execute(self._query + f' LIMIT {limit}').fetchall()
            return cursor.execute(self._query).fetchall()

    def items(self) -> Generator:
        """
        Generator that yields: (column_name, col_object)
        """
        for col in self.columns:
            yield col, getattr(self, col)

    def applymap(self, func: Callable, *, ignore_na: bool = False, args: tuple = tuple(), **kwargs) -> Generator:
        """
        Apply function on each cell in the table

        example:
        db = DataBase(db_path='data/forestation.db')
        table = db.regions.applymap(lambda x: len(x) if isinstance(x, str) else None)
        for row in table:
            print(row)

        (11, 3, 26, 10)
        (6, 3, 18, 19)
        (8, 3, 18, 10)
        (5, 3, 5, None)

        :param func: Callable
        :param ignore_na: bool, default: False
        :param args: tuple, args to pass to the function
        :param kwargs: keyword args to pass to the callable
        :return: Generator
        """
        for row in self:
            yield tuple(cell if cell is None and ignore_na is True else func(cell, *args, **kwargs) for cell in row)

    @property
    def iloc(self) -> IndexLoc:
        """
        Get data by: index, list, or slice

        Getitem supports three ways of indexing table rows:
        1) Singular Integer, ex: IndexIloc[0], IndexIloc[32], or with negative: IndexIloc[-12]
        2) Passing a list of integers, ex: IndexIloc[[1, 22, 4, 3, 17, 38]], IndexIloc[[1, -4, 17, 22, 38, -4, -1]]
        4) Passing Slice, ex: IndexIloc[:10], IndexIloc[2:8], IndexIloc[2:24:2]

        The return type will be a list for multiple items and a tuple for single items

        :return: tuple or list of tuples
        """
        return IndexLoc(it=iter(self), length=len(self))

    def __iter__(self) -> Generator:
        """
        Yield rows from cursor
        """
        with self.conn as cursor:
            yield from cursor.execute(self._query)

    def _get_col(self, column: str) -> Column:
        """
        Get column object

        :param column:str, column name
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
            raise KeyError(f'No such Column: {item}, must be one of the following: {", ".join(self.columns)}')

    def __getattr__(self, attr: str) -> Column:
        """
        Get column object for given column name

        :param attr: str, column-name
        :return: Column
        :raise: AttributeError
        """
        try:
            return self._get_col(attr)
        except InvalidColumnError:
            raise AttributeError(f'No such attribute: {attr}')

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
