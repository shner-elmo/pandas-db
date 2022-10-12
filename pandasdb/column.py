from __future__ import annotations

from pandas import DataFrame, Series

import sqlite3
from typing import Generator, Callable, Any, Sequence, TypeVar, Iterable

from .expression import Expression
from .indexloc import IndexLoc
from .cache import Cache
from .utils import same_val_generator


BaseTypes = str | int | float | bool | None
Numeric = int | float
T = TypeVar('T')


class Column:
    """
    An object that represents a column of a table within a DataBase
    """
    def __init__(self, conn: sqlite3.Connection, cache: Cache, table_name: str, col_name: str) -> None:
        """
        Initialize the Column object

        :param conn: sqlite3.Connection
        :param cache: Cache, instance of Cache
        :param table_name: str
        :param col_name: str
        """
        self.conn = conn
        self._cache = cache  # TODO move column objects to dict and make attributes public
        self.table = table_name
        self.name = col_name
        self.query = f'SELECT {col_name} FROM {table_name}'

    # TODO remove this and use view instead
    @classmethod
    def create_from_query(cls, conn: sqlite3.Connection, cache: Cache, table_name: str, col_name: str,
                          query: str) -> Column:
        """
        Return an instance of Column with a custom query

        :param conn: sqlite3.Connection
        :param cache: Cache, instance of Cache
        :param table_name: str
        :param col_name: str
        :param query: str, SQL query
        :return: Column instance
        """
        column = cls(conn=conn, cache=cache, table_name=table_name, col_name=col_name)
        column._query = query
        return column

    @property
    def type(self) -> type:
        """
        Get column Python data type, i.e: str, int or float

        :return: type, str | int | float
        """
        query = f'{self.query} WHERE {self.name} NOT NULL LIMIT 1'
        out = self._cache.execute(query)[0][0]
        return type(out)

    @property
    def sql_type(self) -> str:
        """
        Get the column SQL data type as a string

        Return a string with the SQL data type, some of the most common are:
        TEXT, INTEGER, REAL, FLOAT, TIMESTAMP, BPCHAR, VARCHAR(250), NUMERIC(10,2), etc.

        :return str, e.g., TEXT, INTEGER, REAL...
        """
        for row in self._cache.execute(f"PRAGMA table_info('{self.table}')"):
            if row[1] == self.name:
                return row[2]

    def data_is_numeric(self) -> bool:
        """
        Return True if the column data is of type int or float, else: False
        """
        return self.type in (int, float)

    @property
    def len(self) -> int:
        """
        Get the amount of rows/ cells in the column (including None values)
        """
        return self._cache.execute(f'SELECT COUNT(*) FROM {self.table}')[0][0]

    def count(self) -> int:
        """
        Get the amount of rows/ cells in the column (excluding None values)
        """
        return self._cache.execute(f'SELECT COUNT({self.name}) FROM {self.table}')[0][0]

    def na_count(self) -> int:
        """
        Get the amount of None values in column
        """
        return self._cache.execute(f'SELECT COUNT(*) FROM {self.table} WHERE {self.name} IS NULL')[0][0]

    def min(self) -> str | int | float:
        """
        Get the min value of the column
        """
        return self._cache.execute(f'SELECT MIN({self.name}) FROM {self.table}')[0][0]

    def max(self) -> str | int | float:
        """
        Get the max value of the column
        """
        return self._cache.execute(f'SELECT MAX({self.name}) FROM {self.table}')[0][0]

    def sum(self) -> Numeric:
        """
        Get the sum of all values within the column

        :raise TypeError: if column isn't of type int or float
        :return float
        """
        if not self.data_is_numeric():
            raise TypeError(f'Cannot get sum for Column of type {self.type}')

        return self._cache.execute(f'SELECT SUM({self.name}) FROM {self.table}')[0][0]

    def avg(self) -> Numeric:
        """
        Get the avg value of the column

        :raise TypeError: if column isn't of type int or float
        :return float
        """
        if not self.data_is_numeric():
            raise TypeError(f'Cannot get avg for Column of type {self.type}')

        return self._cache.execute(f'SELECT AVG({self.name}) FROM {self.table}')[0][0]

    def median(self) -> Numeric:
        """
        Get the median value of the column

        :raise TypeError: if column isn't of type int or float
        :return float
        """
        if not self.data_is_numeric():
            raise TypeError(f'Cannot get median for Column of type {self.type}')

        count = self.count()
        if count % 2 == 0:
            indexes = [count // 2, count // 2 + 1]
            lst = [tup for tup in self.iloc[indexes]]
            avg = sum(lst) / len(lst)
            return avg
        else:
            idx = count // 2 + 1
            return self.iloc[idx]

    def mode(self) -> dict[BaseTypes, int]:
        """
        Get the mode/s of the column as a dictionary; {'value': count}

        :return dict
        """
        query = f"""
        SELECT {self.name}, COUNT(*) FROM {self.table}
        GROUP BY 1
        HAVING COUNT(*) >= (	
            SELECT COUNT(*) FROM {self.table}
            GROUP BY {self.name}
            ORDER BY 1 DESC
            LIMIT 1
        )
        """
        return dict(self._cache.execute(query))

    def describe(self) -> dict[str, str | int | float]:
        """
        Get a dictionary with different properties for the column

        if column data is numeric return a dictionary with keys:
        {'len', 'count', 'min', 'max', 'sum', 'avg', 'median'}
        if its text data:
        {'len', 'count', 'min', 'max', 'mode'}

        :return dict
        """
        if self.data_is_numeric():
            return {
                'len': self.len,
                'count': self.count(),
                'min': self.min(),
                'max': self.max(),
                'sum': self.sum(),
                'avg': self.avg(),
                'median': self.median()
            }
        else:
            return {
                'len': self.len,
                'count': self.count(),
                'min': self.min(),
                'max': self.max(),
                'unique': len(self.unique())
            }

    def unique(self) -> list[BaseTypes]:
        """
        Get list with unique values

        :return list
        """
        return list(tup[0] for tup in self._cache.execute(f'SELECT DISTINCT {self.name} FROM {self.table}'))

    def value_counts(self) -> dict[BaseTypes, int]:
        """
        Get a dictionary with the count of each value in the Column

        example:
        column = ['a', 'b', 'c', 'b', 'c', 'b'] -> {'a': 1, 'b': 3, 'c': 2}

        :return: dict
        """
        query = f"""
        SELECT {self.name}, COUNT(*) FROM {self.table}
        WHERE {self.name} IS NOT NULL
        GROUP BY 1
        ORDER BY 2 DESC, 1 ASC
        """
        return dict(self._cache.execute(query))

    def to_series(self) -> Series:
        """
        Return column as a Pandas Series

        :return Pandas Series
        """
        return Series(data=iter(self), name=self.name)

    def data(self, limit: int = None) -> list[BaseTypes]:
        """
        Get column-data

        If limit is None: return all data, else: return n_amount of rows/ cells

        :param limit: int
        :return: list
        """
        with self.conn as cursor:
            if limit:
                return [tup[0] for tup in cursor.execute(self.query + f' LIMIT {limit}')]
            return [tup[0] for tup in cursor.execute(self.query)]

    def sample(self, n: int = 10) -> list[BaseTypes]:
        """
        Get a list of random values from the column

        :param n: int, number of values
        :return: list
        """
        with self.conn as cursor:
            return [tup[0] for tup in cursor.execute(f'{self.query} ORDER BY RANDOM() LIMIT {n}')]

    def apply(self, func: Callable[[BaseTypes, ...], T], *, ignore_na: bool = True,
              args: tuple = (), **kwargs: Any) -> Generator[T, None, None]:
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
        :param ignore_na: bool, default: True
        :param args: tuple, args to pass to the function
        :param kwargs: keyword arguments to pass to the function
        :return: Generator
        """
        for cell in self:
            if cell is None and ignore_na:
                yield cell
            else:
                yield func(cell, *args, **kwargs)

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
        return IndexLoc(obj=self)

    def filter(self, expression: Expression) -> Column:
        """
        Return a new Column object with the filtered data

        :param expression: Expression
        :return: Column instance
        """
        with self.conn as cursor:
            cursor.execute('CREATE VIEW ')

        view_name = expression.query.replace(' ', '_')
        view_query = f'{self.query} WHERE {expression.query}'

        with self.conn as cursor:
            cursor.execute(f'CREATE VIEW {view_name} AS {view_query}')

        return Column(conn=self.conn, cache=self._cache, table_name=view_name, col_name=self.name)

    def __getitem__(self, item: int | slice | list | Expression) -> Any:
        """
        Return index slice or filtered Column

        You can do two things with Column.__getitem__():
        1. Get value/s at given index
        2. Get a filtered Column

        There are three ways to get a value or list of values at a given index:
        1. pass an integer: db.table.column[28]
        2. passing a slice: db.table.column[8:24:2]
        3. and using a list: db.table.column[[3, 2, 8, -1, 15]]

        And for filtering a column you can simply pass a column with a logical expression:
        col1 = db.table.col1
        col1[col1 > 10]

        :param item: int | slice | list | Expression
        :return: IndexLoc | Column
        """
        if isinstance(item, Expression):
            return self.filter(item)
        elif isinstance(item, (int, slice, list)):
            return self.iloc[item]

        raise TypeError(f'param item must be of type Expression, int, slice, or list. not: {type(item)}')

    def __iter__(self) -> Generator[BaseTypes, None, None]:
        """ Yield values from column """
        with self.conn as cursor:
            for i in cursor.execute(self.query):
                yield i[0]

    def __len__(self) -> int:
        """ Get amount of rows """
        return self.len

    def __hash__(self) -> int:
        """ Get hash value of Column """
        return hash(f'{self.table}.{self.name}')

    def _repr_df(self) -> DataFrame:
        """
        Convert column to Dataframe

        Convert column to a Dataframe but only first and last five rows,
        without iterating through each row.

        :return: DataFrame
        """
        top_rows = 10
        bottom_rows = 10

        data = self.iloc[:top_rows] + self.iloc[-bottom_rows:]
        n = len(self)
        index = list(range(top_rows)) + list(range(n - bottom_rows, n))
        return DataFrame(index=index, data=data, columns=[self.name])

    def __repr__(self) -> str:
        """ Return column as a Pandas Series """
        return self._repr_df().to_string(show_dimensions=False, max_rows=10)

    def _repr_html_(self) -> str:
        """ Return column in HTML """
        return self._repr_df().to_html(show_dimensions=False, max_rows=10)

    def __add__(self, other: Column | Iterable | str | int | float | bool) -> Generator:
        """
        Return a generator with the arithmetic operation applied on each element

        :param other: Column | Iterable | str | int | float | bool
        :return: Generator
        """
        if isinstance(other, str) or not isinstance(other, Iterable):
            other = same_val_generator(val=other, size=len(self))

        for x, y in zip(self, other, strict=True):
            if x is None:
                yield x
            else:
                yield x + y

    def __sub__(self, other: Column | Iterable | Numeric) -> Generator:
        """
        Return a generator with the arithmetic operation applied on each element

        :param other: Column | Iterable | float | int
        :return: None
        """
        if not isinstance(other, Iterable):
            other = same_val_generator(val=other, size=len(self))

        for x, y in zip(self, other, strict=True):
            if x is None:
                yield x
            else:
                yield x - y

    def __mul__(self, other: Column | Iterable | Numeric) -> Generator:
        """
        Return a generator with the arithmetic operation applied on each element

        :param other: Column | Iterable | float | int
        :return: None
        """
        if not isinstance(other, Iterable):
            other = same_val_generator(val=other, size=len(self))

        for x, y in zip(self, other, strict=True):
            if x is None:
                yield x
            else:
                yield x * y

    def __truediv__(self, other: Column | Iterable | Numeric) -> Generator:
        """
        Return a generator with the arithmetic operation applied on each element

        :param other: Column | Iterable | float | int
        :return: None
        """
        if not isinstance(other, Iterable):
            other = same_val_generator(val=other, size=len(self))

        for x, y in zip(self, other, strict=True):
            if x is None:
                yield x
            else:
                yield x / y

    def __floordiv__(self, other: Column | Iterable | Numeric) -> Generator:
        """
        Return a generator with the arithmetic operation applied on each element

        :param other: Column | Iterable | float | int
        :return: None
        """
        if not isinstance(other, Iterable):
            other = same_val_generator(val=other, size=len(self))

        for x, y in zip(self, other, strict=True):
            if x is None:
                yield x
            else:
                yield x // y

    # TODO: complete expressions docstrings
    def __gt__(self, other: Numeric) -> Expression:
        """

        :param other: float
        :return: Expression
        """
        return Expression(query=f'{self.name} > {other} ', table=self.table)

    def __ge__(self, other: Numeric) -> Expression:
        """

        :param other: float
        :return: Expression
        """
        return Expression(query=f'{self.name} >= {other} ', table=self.table)

    def __lt__(self, other: Numeric) -> Expression:
        """

        :param other: float
        :return: Expression
        """
        return Expression(query=f'{self.name} < {other} ', table=self.table)

    def __le__(self, other: Numeric) -> Expression:
        """

        :param other: float
        :return: Expression
        """
        return Expression(query=f'{self.name} <= {other} ', table=self.table)

    def __eq__(self, other: BaseTypes) -> Expression:
        """

        :param other: str or float
        :return: Expression
        """
        if isinstance(other, str):
            return Expression(query=f"{self.name} = '{other}' ", table=self.table)
        return Expression(query=f'{self.name} = {other} ', table=self.table)

    def __ne__(self, other: BaseTypes) -> Expression:
        """

        :param other: str or float
        :return: Expression
        """
        if isinstance(other, str):
            return Expression(query=f"{self.name} != '{other}' ", table=self.table)
        return Expression(query=f'{self.name} != {other} ', table=self.table)

    def isin(self, options: Sequence) -> Expression:
        """

        :param options: tuple
        :return: Expression
        """
        if not isinstance(options, tuple):
            options = tuple(options)
        return Expression(query=f'{self.name} IN {options} ', table=self.table)

    def between(self, x: Numeric, y: Numeric) -> Expression:
        """

        :param x: float
        :param y: float
        :return: Expression
        """
        return Expression(query=f'{self.name} BETWEEN {x} AND {y} ', table=self.table)

    def like(self, regex: str) -> Expression:
        """

        :param regex: str
        :return: Expression
        """
        return Expression(query=f"{self.name} LIKE '{regex}' ", table=self.table)

    def ilike(self, regex: str) -> Expression:
        """

        :param regex: str
        :return: Expression
        """
        return Expression(query=f"{self.name} ILIKE '{regex}' ", table=self.table)
