from pandas import DataFrame, Series

import sqlite3
from typing import Generator

from exceptions import ColumnError, ExpressionError


class Expression:
    """
    A class for converting Python's logical operators to SQL-compatible strings
    """
    def __init__(self, query: str):
        self.query = query

    def __and__(self, expression):
        if not isinstance(expression, Expression):
            raise ExpressionError('expression must be an instance of Expression, try using a column object instead')

        return Expression(query=f'{self.query} AND {expression.query} ')

    def __or__(self, expression):
        if not isinstance(expression, Expression):
            raise ExpressionError('expression must be an instance of Expression, try using a column object instead')

        return Expression(query=f'{self.query} OR {expression.query} ')

    def __str__(self) -> str:
        return __class__.__name__ + f'("{self.query}")'

    def __repr__(self) -> str:
        return __class__.__name__ + f'("{self.query}")'


class Column:
    """
    An object that represents a column of a table within a DataBase
    """
    def __init__(self, cursor: sqlite3.Cursor, table_name: str, col_name: str) -> None:
        self.cursor = cursor
        self.table = table_name
        self.name = col_name
        self.query = f'SELECT {col_name} FROM {table_name}'

    @property
    def type(self) -> str:
        """
        Get column type
        """
        for row in self.cursor.execute(f"PRAGMA table_info('{self.table}')"):
            if row[1] == self.name:
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
        return Series(data=self.data(), name=self.name)

    def data(self, limit: int = None) -> list:
        """
        Get column-data

        If limit is None: return all data, else: return n_amount of rows/ cells

        :param limit: int
        :return: list
        """
        if limit:
            return [x[0] for x in self.cursor.execute(self.query + f' LIMIT {limit}')]
        return [x[0] for x in self.cursor.execute(self.query)]

    def __iter__(self) -> Generator:
        for i in self.cursor.execute(self.query):
            yield i[0]

    def __len__(self) -> int:
        return self.len

    def __str__(self) -> str:
        """ Return column as a Pandas Series """
        return self.to_series().to_string(max_rows=10, index=True, name=True, length=True, dtype=True)

    def __repr__(self) -> str:
        """ Return column as a Pandas Series """
        return self.to_series().to_string(max_rows=10, index=True, name=True, length=True, dtype=True)

    def __gt__(self, other):
        return Expression(query=f'{self.table}.{self.name} > {other} ')

    def __ge__(self, other):
        return Expression(query=f'{self.table}.{self.name} >= {other} ')

    def __lt__(self, other):
        return Expression(query=f'{self.table}.{self.name} < {other} ')

    def __le__(self, other):
        return Expression(query=f'{self.table}.{self.name} <= {other} ')

    def __eq__(self, other):
        if type(other) is str:
            return Expression(query=f"{self.table}.{self.name} = '{other}' ")
        return Expression(query=f'{self.table}.{self.name} = {other} ')

    def __ne__(self, other):
        if type(other) is str:
            return Expression(query=f"{self.table}.{self.name} != '{other}' ")
        return Expression(query=f'{self.table}.{self.name} != {other} ')

    def isin(self, options: tuple):
        if type(options) is not tuple:
            options = tuple(options)
        return Expression(query=f'{self.table}.{self.name} IN {options} ')

    def between(self, x, y):
        return Expression(query=f'{self.table}.{self.name} BETWEEN {x} AND {y} ')

    def like(self, regex):
        return Expression(query=f"{self.table}.{self.name} LIKE '{regex}' ")

    def ilike(self, regex):
        return Expression(query=f"{self.table}.{self.name} ILIKE '{regex}' ")


class Table:
    """
    An object that represents an SQL table
    """
    def __init__(self, cursor: sqlite3.Cursor, name: str) -> None:
        self.cursor = cursor
        self.name = name
        self.query = f'SELECT * FROM {self.name}'

        for col in self.columns:
            setattr(self, col, Column(cursor=self.cursor, table_name=self.name, col_name=col))

    @property
    def columns(self) -> list[str]:
        """
        Get list with column names
        """
        return [x[1] for x in self.cursor.execute(f"PRAGMA table_info('{self.name}')")]

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
        if limit:
            return self.cursor.execute(self.query + f' LIMIT {limit}').fetchall()
        return self.cursor.execute(self.query).fetchall()

    def items(self) -> Generator:
        """
        Generator that yields: (column_name, col_object)
        """
        for col in self.columns:
            yield col, Column(cursor=self.cursor, table_name=self.name, col_name=col)

    def __iter__(self) -> Generator:
        """
        Yield rows from cursor
        """
        for row in self.cursor.execute(self.query):
            yield row

    def _get_col(self, column):
        if column not in self.columns:
            raise ColumnError(f'Column must be one of the following: {", ".join(self.columns)}')
        return getattr(self, column)

    def __getitem__(self, item) -> Column:
        """
        Get column object for given column name

        :param item: str, column-name
        :return: Column
        """
        return self._get_col(item)

    def __getattr__(self, item) -> Column:
        """
        Get column object for given column name

        :param item: str, column-name
        :return: Column
        """
        return self._get_col(item)

    def __len__(self) -> int:
        """ Return amount of rows """
        return self.len

    def __str__(self) -> str:
        """ Return table as a Pandas DataFrame """
        return self.to_df().to_string(max_rows=10, max_cols=10, show_dimensions=True)

    def __repr__(self) -> str:
        """ Return table as a Pandas DataFrame """
        return self.to_df().to_string(max_rows=10, max_cols=10, show_dimensions=True)
