from __future__ import annotations

from pandas import DataFrame, Series

import sqlite3
from typing import Generator, Callable, Any, Sequence

from .expression import Expression
from .indexloc import IndexLoc
from .cache import Cache

BaseType = str | int | float | bool | None
Numeric = int | float


class Column:
    """
    An object that represents a column of a table within a DataBase
    """
    def __init__(self, conn: sqlite3.Connection, cache: Cache, table_name: str, col_name: str) -> None:
        """
        # TODO complete docstring

        :param conn:
        :param cache:
        :param table_name:
        :param col_name:
        """
        self.conn = conn  # make all attributes private
        self._cache = cache
        self._table = table_name
        self._name = col_name
        self._query = f'SELECT {col_name} FROM {table_name}'

    @property
    def type(self) -> type:
        """
        Get column Python data type, i.e: str, int or float

        :return: type, str | int | float
        """
        return type(next(iter(self)))  # TODO: test how long to get teh first element in a big and small table

    @property
    def sql_type(self) -> str:
        """
        Get the column SQL data type as a string

        Return a string with the SQL data type, some of the most common are:
        TEXT, INTEGER, REAL, FLOAT, TIMESTAMP, BPCHAR, VARCHAR(250), NUMERIC(10,2), etc.

        :return str, e.g., TEXT, INTEGER, REAL...
        """
        for row in self._cache.execute(f"PRAGMA table_info('{self._table}')"):
            if row[1] == self._name:
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
        return self._cache.execute(f'SELECT COUNT(*) FROM {self._table}')[0][0]

    def count(self) -> int:
        """
        Get the amount of rows/ cells in the column (excluding None values)
        """
        return self._cache.execute(f'SELECT COUNT({self._name}) FROM {self._table}')[0][0]

    def na_count(self) -> int:
        """
        Get the amount of None values in column
        """
        return self._cache.execute(f'SELECT COUNT(*) FROM {self._table} WHERE {self._name} IS NULL')[0][0]

    def min(self) -> BaseType:
        """
        Get the min value of the column
        """
        return self._cache.execute(f'SELECT MIN({self._name}) FROM {self._table}')[0][0]

    def max(self) -> BaseType:
        """
        Get the max value of the column
        """
        return self._cache.execute(f'SELECT MAX({self._name}) FROM {self._table}')[0][0]

    def sum(self) -> float:
        """
        Get the sum of all values within the column

        :raise TypeError: if column isn't of type int or float
        :return float
        """
        if not self.data_is_numeric():
            raise TypeError(f'Cannot get sum for Column of type {self.type}')

        return self._cache.execute(f'SELECT SUM({self._name}) FROM {self._table}')[0][0]

    def avg(self) -> float:
        """
        Get the avg value of the column

        :raise TypeError: if column isn't of type int or float
        :return float
        """
        if not self.data_is_numeric():
            raise TypeError(f'Cannot get avg for Column of type {self.type}')

        return self._cache.execute(f'SELECT AVG({self._name}) FROM {self._table}')[0][0]

    def median(self) -> float:
        """
        Get the median value of the column

        :raise TypeError: if column isn't of type int or float
        :return float
        """
        if not self.data_is_numeric():
            raise TypeError(f'Cannot get median for Column of type {self.type}')

        def get_row_by_index(index: int | tuple) -> list[tuple]:
            """
            You can pass the index as either an integer or a tuple of integers
            """
            row_id = f'== {index}' if isinstance(index, int) else f'IN {index}'
            q = f"""
            SELECT {self._name} FROM (
                SELECT 
                    {self._name}, 
                    ROW_NUMBER() OVER(ORDER BY {self._name} DESC) AS row_id  -- DESC to get NULL values last
                FROM {self._table} 
            )
            WHERE row_id {row_id}
            """
            return self._cache.execute(q)

        count = self.count()
        if count % 2 == 0:
            indexes = (count // 2, count // 2 + 1)
            lst = [tup[0] for tup in get_row_by_index(indexes)]
            avg = sum(lst) / len(lst)
            return avg
        else:
            idx = count // 2 + 1
            return get_row_by_index(idx)[0][0]

    def mode(self) -> dict[Any, int]:
        """
        Get the mode/s of the column as a dictionary; {'value': count}

        :return dict
        """
        query = f"""
        SELECT {self._name}, COUNT(*) FROM {self._table}
        GROUP BY 1
        HAVING COUNT(*) >= (	
            SELECT COUNT(*) FROM {self._table}
            GROUP BY {self._name}
            ORDER BY 1 DESC
            LIMIT 1
        )
        """
        return dict(self._cache.execute(query))

    def describe(self) -> dict[str, BaseType]:
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

    def unique(self) -> list:
        """
        Get list with unique values

        :return list
        """
        return list(tup[0] for tup in self._cache.execute(f'SELECT DISTINCT {self._name} FROM {self._table}'))

    def has_duplicates(self) -> bool:
        """
        Return True if columns has duplicated values

        :return: bool
        """
        unique_count = self._cache.execute(f'SELECT COUNT(DISTINCT {self._name}) FROM {self._table}')[0][0]
        return len(self) != unique_count

    def value_counts(self) -> dict[Any, int]:
        """
        Get a dictionary with the count of each value in the Column

        example:
        column = ['a', 'b', 'c', 'b', 'c', 'b'] -> {'a': 1, 'b': 3, 'c': 2}

        :return: dict
        """
        query = f"""
        SELECT {self._name}, COUNT(*) FROM {self._table}
        WHERE {self._name} IS NOT NULL
        GROUP BY 1
        ORDER BY 2 DESC, 1 ASC
        """
        return dict(self._cache.execute(query))

    def to_series(self) -> Series:
        """
        Return column as a Pandas Series

        :return Pandas Series
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
                return [tup[0] for tup in cursor.execute(self._query + f' LIMIT {limit}')]
            return [tup[0] for tup in cursor.execute(self._query)]

    def apply(self, func: Callable, *, ignore_na: bool = True, args: tuple = tuple(), **kwargs) -> Generator:
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
        return IndexLoc(it=iter(self), length=len(self))
    
    # def filter(self, expression: Expression, return_df: bool = False) -> Generator:
    #     """
    #     Return a generator with the filtered data
    #
    #     :param expression: _description_
    #     :type expression: Expression
    #     :yield: _description_
    #     :return: Generator
    #     """
    #     expression_column = ''
    #     if expression_column not in self.columns:
    #         raise ValueError('Filter Column must be in ')
    #
    #     query = f""" """
    #
    #     with self.conn as cursor:
    #         cur = cursor.execute(query)  # replace with self._cache.execute()
    #
    #     if return_df:
    #         return DataFrame(data=cur)
        
    def __getitem__(self, item: int | slice | list):  # -> list | str | int | float
        """ Return index slice """
        # if isinstance(item, Expression):
        #     return self.filter()
        return self.iloc[item]

    def __iter__(self) -> Generator[BaseType, None, None]:
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

    def _repr_df(self) -> DataFrame:
        """
        Convert column to Dataframe

        Convert column to Dataframe but with only first and last five rows,
        without iterating through each row.

        :return: DataFrame
        """
        max_rows = 10
        n_rows = len(self)

        index_col: list[str | int] = ['__index_col__']
        if n_rows > max_rows:
            index_col.extend(range(max_rows // 2))
            index_col.append('...')
            index_col.extend(range(n_rows - (max_rows // 2), n_rows))
        else:
            index_col.extend(range(n_rows))

        cols: list[list] = [index_col]

        col_data: list[BaseType] = [self._name]
        if n_rows > max_rows:
            col_data.extend(self.iloc[:max_rows // 2])
            col_data.append('...')
            col_data.extend(self.iloc[-max_rows // 2:])
        else:
            col_data.extend(self.iloc[:])

        cols.append(col_data)

        data = list(zip(*cols))  # transpose nested list; [(col1), (col2)] -> [(row1), (row2), (row3)...]
        df = DataFrame(data=data[1:], columns=data[0])
        df = df.set_index('__index_col__')
        df.index.name = None
        return df

    def __str__(self) -> str:
        """ Return column as a Pandas Series """
        return self._repr_df().to_string(show_dimensions=False)

    def __repr__(self) -> str:
        """ Return column as a Pandas Series """
        return self._repr_df().to_string(show_dimensions=False)

    def _repr_html_(self) -> str:
        """ Return column in HTML """
        return self._repr_df().to_html(show_dimensions=False)

    # TODO add docstrings for math functions
    def __add__(self, other: Column | BaseType) -> Generator:  # TODO change to -> Column ? and check others
        """
        Return a generator with the arithmetic operation applied on each element

        :param other: Column | int | float | str
        :return: Generator
        """
        if isinstance(other, Column):
            if self.data_is_numeric() and other.data_is_numeric():
                pass
            elif self.type is str and other.type is str:
                pass
            else:
                raise TypeError(f'Unsupported operand types for + ({self.type} and {other.type})')

            for x, y in zip(self, other):
                yield x + y
        else:
            if isinstance(self, (int, float)) and isinstance(other, (int, float)):
                pass
            elif self.type is str and isinstance(other, str):
                pass
            else:
                raise TypeError(f'Unsupported operand types for + ({self.type} and {type(other)})')

            for x in self:
                yield x + other

    def __sub__(self, other: Column | float) -> Generator:
        """
        Return a generator with the arithmetic operation applied on each element

        :param other: Column | float
        :return: None
        """
        if not self.data_is_numeric():
            raise TypeError(f'Cannot perform arithmetic operation with non-numerical data (type {self.type})')

        if isinstance(other, Column):
            if not other.data_is_numeric():
                raise TypeError(f'Unsupported operand for types {self.type} and {other.type}')

            for x, y in zip(self, other):
                yield x - y
        else:
            if not isinstance(other, (int, float)):
                raise TypeError(f'Cannot perform arithmetic operation with non-numerical data (type {self.type})')

            for x in self:
                yield x - other

    def __mul__(self, other: Column | float) -> Generator:
        """
        Return a generator with the arithmetic operation applied on each element

        :param other: Column | float
        :return: None
        """
        if not self.data_is_numeric():
            raise TypeError(f'Cannot perform arithmetic operation with non-numerical data (type {self.type})')

        if isinstance(other, Column):
            if not other.data_is_numeric():
                raise TypeError(f'Unsupported operand for types {self.type} and {other.type}')

            for x, y in zip(self, other):
                yield x * y
        else:
            if not isinstance(other, (int, float)):
                raise TypeError(f'Cannot perform arithmetic operation with non-numerical data (type {self.type})')

            for x in self:
                yield x * other

    def __truediv__(self, other: Column | float) -> Generator:
        """
        Return a generator with the arithmetic operation applied on each element

        :param other: Column | float
        :return: None
        """
        if not self.data_is_numeric():
            raise TypeError(f'Cannot perform arithmetic operation with non-numerical data (type {self.type})')

        if isinstance(other, Column):
            if not other.data_is_numeric():
                raise TypeError(f'Unsupported operand for types {self.type} and {other.type}')

            for x, y in zip(self, other):
                yield x / y
        else:
            if not isinstance(other, (int, float)):
                raise TypeError(f'Cannot perform arithmetic operation with non-numerical data (type {self.type})')

            for x in self:
                yield x / other

    def __floordiv__(self, other: Column | float) -> Generator:
        """
        Return a generator with the arithmetic operation applied on each element

        :param other: Column | float
        :return: None
        """
        if not self.data_is_numeric():
            raise TypeError(f'Cannot perform arithmetic operation with non-numerical data (type {self.type})')

        if isinstance(other, Column):
            if not other.data_is_numeric():
                raise TypeError(f'Unsupported operand for types {self.type} and {other.type}')

            for x, y in zip(self, other):
                yield x // y
        else:
            if not isinstance(other, (int, float)):
                raise TypeError(f'Cannot perform arithmetic operation with non-numerical data (type {self.type})')

            for x in self:
                yield x // other

    # TODO: complete expressions docstrings
    def __gt__(self, other: Numeric) -> Expression:
        """

        :param other: float
        :return: Expression
        """
        return Expression(query=f'{self._table}.{self._name} > {other} ')

    def __ge__(self, other: Numeric) -> Expression:
        """

        :param other: float
        :return: Expression
        """
        return Expression(query=f'{self._table}.{self._name} >= {other} ')

    def __lt__(self, other: Numeric) -> Expression:
        """

        :param other: float
        :return: Expression
        """
        return Expression(query=f'{self._table}.{self._name} < {other} ')

    def __le__(self, other: Numeric) -> Expression:
        """

        :param other: float
        :return: Expression
        """
        return Expression(query=f'{self._table}.{self._name} <= {other} ')

    def __eq__(self, other: BaseType) -> Expression:
        """

        :param other: str or float
        :return: Expression
        """
        if isinstance(other, str):
            return Expression(query=f"{self._table}.{self._name} = '{other}' ")
        return Expression(query=f'{self._table}.{self._name} = {other} ')

    def __ne__(self, other: BaseType) -> Expression:
        """

        :param other: str or float
        :return: Expression
        """
        if isinstance(other, str):
            return Expression(query=f"{self._table}.{self._name} != '{other}' ")
        return Expression(query=f'{self._table}.{self._name} != {other} ')

    def isin(self, options: Sequence) -> Expression:
        """

        :param options: tuple
        :return: Expression
        """
        if not isinstance(options, tuple):
            options = tuple(options)
        return Expression(query=f'{self._table}.{self._name} IN {options} ')

    def between(self, x: Numeric, y: Numeric) -> Expression:
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
