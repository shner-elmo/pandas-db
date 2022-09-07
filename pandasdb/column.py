from pandas import Series

import sqlite3
from typing import Generator, Callable, Any

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
    def type(self) -> type:
        """
        Get column Python data type, i.e: str, int or float

        :return: type, str | int | float
        """
        return type(next(iter(self)))

    @property
    def sql_type(self) -> str:
        """
        Get the column SQL data type as a string

        Return a string with the SQL data type, some of the most common are:
        TEXT, INTEGER, REAL, FLOAT, TIMESTAMP, BPCHAR, VARCHAR(250), NUMERIC(10,2), etc.

        :return str, e.g., TEXT, INTEGER, REAL...
        """
        with self.conn as cursor:
            for row in cursor.execute(f"PRAGMA table_info('{self._table}')"):
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
        with self.conn as cursor:
            return cursor.execute(f'SELECT COUNT(*) FROM {self._table}').fetchone()[0]

    def count(self) -> int:
        """
        Get the amount of rows/ cells in the column (excluding None values)
        """
        with self.conn as cursor:
            return cursor.execute(f'SELECT COUNT({self._name}) FROM {self._table}').fetchone()[0]

    def na_count(self) -> int:
        """
        Get the amount of None values in column
        """
        with self.conn as cursor:
            return cursor.execute(f'SELECT COUNT(*) FROM {self._table} WHERE {self._name} IS NULL').fetchone()[0]

    def min(self) -> int | float | str:
        """
        Get the min value of the column
        """
        with self.conn as cursor:
            return cursor.execute(f'SELECT MIN({self._name}) FROM {self._table}').fetchone()[0]

    def max(self) -> int | float | str:
        """
        Get the max value of the column
        """
        with self.conn as cursor:
            return cursor.execute(f'SELECT MAX({self._name}) FROM {self._table}').fetchone()[0]

    def sum(self) -> float:
        """
        Get the sum of all values within the column

        :raise TypeError: if column isn't of type int or float
        :return float
        """
        if not self.data_is_numeric():
            raise TypeError(f'Cannot get sum for Column of type {self.type}')

        with self.conn as cursor:
            return cursor.execute(f'SELECT SUM({self._name}) FROM {self._table}').fetchone()[0]

    def avg(self) -> float:
        """
        Get the avg value of the column

        :raise TypeError: if column isn't of type int or float
        :return float
        """
        if not self.data_is_numeric():
            raise TypeError(f'Cannot get avg for Column of type {self.type}')

        with self.conn as cursor:
            return cursor.execute(f'SELECT AVG({self._name}) FROM {self._table}').fetchone()[0]

    def median(self) -> float:
        """
        Get the median value of the column

        :raise TypeError: if column isn't of type int or float
        :return float
        """
        if not self.data_is_numeric():
            raise TypeError(f'Cannot get median for Column of type {self.type}')

        def get_row_by_index(index):
            q = f"""
            SELECT {self._name} FROM (
                SELECT 
                    {self._name}, 
                    ROW_NUMBER() OVER(ORDER BY {self._name} DESC) AS row_id  -- DESC to get NULL values last
                FROM {self._table} 
            )
            WHERE row_id == {index}
            """
            with self.conn as cur:
                return cur.execute(q).fetchone()[0]

        count = self.count()
        if count % 2 == 0:
            indexes = (count // 2, count // 2 + 1)
            lst = [get_row_by_index(idx) for idx in indexes]
            avg = sum(lst) / len(lst)
            return avg
        else:
            idx = count // 2 + 1
            return get_row_by_index(idx)

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
        with self.conn as cursor:
            return dict(cursor.execute(query))

    def describe(self) -> dict[str, float]:
        """
        Get a dictionary with different properties for the column

        if column data is numeric return a dictionary with keys:
        {'len', 'count', 'min', 'max', 'sum', 'avg', 'median'}
        if its text data:
        {'len', 'count', 'min', 'max', 'mode'}

        :return
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
        with self.conn as cursor:
            return list(tup[0] for tup in cursor.execute(f'SELECT DISTINCT {self._name} FROM {self._table}'))

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
        ORDER BY 1 ASC, 2 DESC
        """
        with self.conn as cursor:
            return dict(cursor.execute(query))

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
                return [x[0] for x in cursor.execute(self._query + f' LIMIT {limit}')]
            return [x[0] for x in cursor.execute(self._query)]

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

        :param args:
        :param func: Callable
        :param ignore_na: bool, default: True
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
