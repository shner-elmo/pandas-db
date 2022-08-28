from pandas import DataFrame, Series

import sqlite3
from typing import Generator

from .exceptions import InvalidColumnError, ExpressionError


class Expression:
    """
    A class for converting Python's logical operators to SQL-compatible strings
    """
    def __init__(self, query: str) -> None:
        self.query = query

    def __and__(self, expression: 'Expression') -> 'Expression':
        """
        Return an Expression object with 'self.query' as: 'Expression1 AND Expression2'

        :param expression: Expression
        :return: Expression
        :raise: ExpressionError if param: expression isn't an instance of Expression
        """
        if not isinstance(expression, Expression):
            raise ExpressionError('expression must be an instance of Expression, try using a column object instead')

        return Expression(query=f'{self.query} AND {expression.query} ')

    def __or__(self, expression: 'Expression') -> 'Expression':
        """
        Return an Expression object with 'self.query' as: 'Expression1 OR Expression2'

        :param expression: Expression
        :return: Expression
        :raise: ExpressionError if param: expression isn't an instance of Expression
        """
        if not isinstance(expression, Expression):
            raise ExpressionError('expression must be an instance of Expression, try using a column object instead')

        return Expression(query=f'{self.query} OR {expression.query} ')

    def __str__(self) -> str:
        """ Get string representation of Expression instance """
        return __class__.__name__ + f'(query="{self.query}")'

    def __repr__(self) -> str:
        """ Get string representation of Expression instance """
        return __class__.__name__ + f'(query="{self.query}")'


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
