from __future__ import annotations

from pandas import DataFrame, Series

import sqlite3
import itertools
from typing import Generator, Callable, Any, Sequence, TypeVar, Iterable, overload

from .expression import Expression
from .cache import Cache
from .utils import create_temp_view, get_random_name, sql_tuple, convert_type_to_sql

ColumnValue = str | int | float | bool | None
Numeric = int | float
T = TypeVar('T')
ROWID = '_rowid_ AS _rowid_'  # select rowid/oid/_rowid_ as _rowid_


class IndexLoc:
    def __init__(self, column: Column) -> None:
        self.col = column
        self.len = len(column)  # to avoid recomputing

    def index_abs(self, idx: int) -> int:
        """
        Return the absolute of an index

        if the given index is negative it will convert it to positive, for ex:
        if the given index is -1 and the length of the table is 371 the method will return 371

        :param idx: int
        :return: int
        """
        if idx < 0:
            return self.len + idx
        return idx

    def validate_index(self, idx: int) -> None:
        """
        Assert given index is above zero, and below or equal to length,
        else: raise IndexError

        :param idx: int, must be positive
        :raise IndexError: if not 0 <= index < length
        :return: None
        """
        if not 0 <= idx < self.len:
            raise IndexError(f'Given index out of range ({idx})')

    @overload
    def __getitem__(self, index: int) -> ColumnValue:
        ...

    @overload
    def __getitem__(self, index: slice | list) -> list[ColumnValue]:
        ...

    def __getitem__(self, index: int | slice | list) -> ColumnValue | list[ColumnValue]:
        """
        Get row/value at given index

        if an int is passed; then values at the given index will be returned
        if a slice or a list is passed; then it will return a list of elements/cells from the column

        Note that the index is always increased by one before being passed to the SQL query,
        as the rowid columns starts the count at 1 and not 0.

        :param index: int | slice | list
        :return: tuple | list | BaseTypes
        """
        if isinstance(index, int):
            index = self.index_abs(index)
            self.validate_index(index)
            index += 1

            with self.col.conn as cursor:
                query = f'SELECT {self.col.name} FROM {self.col.table} WHERE _rowid_ == {index}'
                return cursor.execute(query).fetchall()[0][0]

        if isinstance(index, slice):
            indices = index.indices(self.len)
            indexes = [idx + 1 for idx in range(*indices)]

            with self.col.conn as cursor:
                query = f'SELECT {self.col.name} FROM {self.col.table} WHERE _rowid_ IN {sql_tuple(indexes)}'
                return [tup[0] for tup in cursor.execute(query)]

        if isinstance(index, list):
            indexes = [self.index_abs(idx) for idx in index]
            for idx in indexes:
                self.validate_index(idx)
            indexes = [idx + 1 for idx in indexes]
            unique_indexes = set(indexes)

            with self.col.conn as cursor:
                rows = cursor.execute(
                    f'SELECT _rowid_, {self.col.name} FROM {self.col.table} '
                    f'WHERE _rowid_ IN {sql_tuple(unique_indexes)}')

            idx_row_mapping: dict[int, ColumnValue] = dict(rows)
            return [idx_row_mapping[idx] for idx in indexes]

        raise TypeError(f'Index must be of type: int, list, or slice. not: {type(index)}')


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
        self._cache = cache
        self.table = table_name
        self.name = col_name
        self.query = f'SELECT {col_name} FROM {table_name}'

    @property
    def type(self) -> type:
        """
        Get column Python data type, i.e: str, int or float

        :return: type, str | int | float
        """
        out = self._cache.execute(f'{self.query} WHERE {self.name} IS NOT NULL LIMIT 1')[0][0]
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

    def null_count(self) -> int:
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

        if self.null_count() == 0:
            col = self.sort_values()
        else:
            col = self.not_null().sort_values()

        n = len(col)
        mid = (n - 1) // 2
        if n % 2 == 0:
            lst: list[Numeric] = col.iloc[[mid, mid + 1]]
            return sum(lst) / 2
        else:
            return col.iloc[mid]

    def mode(self) -> dict[ColumnValue, int]:
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

    def unique(self) -> list[ColumnValue]:
        """
        Get list with unique values

        :return list
        """
        return [tup[0] for tup in self._cache.execute(f'SELECT DISTINCT {self.name} FROM {self.table}')]

    def value_counts(self) -> dict[ColumnValue, int]:
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

    def data(self, limit: int = None) -> list[ColumnValue]:
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

    def sample(self, n: int = 10) -> list[ColumnValue]:
        """
        Get a list of random values from the column

        :param n: int, number of values
        :return: list
        """
        with self.conn as cursor:
            return [tup[0] for tup in cursor.execute(f'{self.query} ORDER BY RANDOM() LIMIT {n}')]

    def apply(self, func: Callable[[ColumnValue, ...], T], *, ignore_na: bool = True,
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
        return IndexLoc(self)

    def not_null(self) -> ColumnView:
        """
        Return a new column without any null values

        :return: ColumnView
        """
        return self.filter(expression=Expression(query=f'{self.name} IS NOT NULL', table=self.table))

    def sort_values(self, ascending: bool = True) -> ColumnView:
        """
        Return a new column with the data sorted

        :param ascending: bool, default True
        :return: ColumnView
        """
        view_name = f'_col_sorted_{self.table}_{self.name}_{get_random_name(size=10)}_'
        query = f"""
        SELECT
            ROW_NUMBER() OVER (ORDER BY {self.name} {'ASC' if ascending else 'DESC'}) AS _rowid_, 
            {self.name}
        FROM {self.table} 
        """
        return self._create_and_get_temp_view(view_name=view_name, query=query)

    def limit(self, n: int) -> ColumnView:
        """
        Return a new column limits the amount of rows in a column

        (creates a view with: f"SELECT ... LIMIT {n}")

        :param n: int
        :return: ColumnView
        """
        view_name = f'_col_sorted_{self.table}_{self.name}_{get_random_name(size=10)}_'
        query = f"SELECT {ROWID}, {self.name} FROM {self.table} WHERE _rowid_ <= {n}"
        return self._create_and_get_temp_view(view_name=view_name, query=query)

    def filter(self, expression: Expression) -> ColumnView:
        """
        Return a new Column object with the filtered data

        - How it works:

        It takes an Expression, which could be either as an instance of the Expression class, for ex:
        `db.table.country.filter(Expression(query='country IN ("USA", "JPY", "ITA")'))`

        or from using an operator on a Column object which will return an instance of Expression, for ex:
        `col = db.table.col`
        `col.filter(col > 35.93)` # return a new column with only records which have `col` above 35.93

        or using Column.isin():
        `countries = db.countries`
        # return a column showing the population for the countries: USA, JPY, and ITA.
        `countries.population.filter(countries.isin(("USA", "JPY", "ITA")))`

        You can also pass the expression with square brackets to the column:
        `col = db.table.col`
        `col[col > 35.93]`

        Once the expression is passed, an SQL view will be created with the query being:
        "SELECT {col} FROM {table} WHERE {expression}"
        and then a new Column instance is returned with the name being the view-name

        --------------------------------------------------------------------------------------------------------
        the view name will start with '_col_` since the data within the table-view represents a column,
        and we add 10 random letters to the name, so we can create other filters for the same column without
        having to delete previous ones, or overwrite them.

        note that the created view is temporary, meaning that when the user closes the connection
        with `db.exit()`, all the views in the list will be dropped/ deleted automatically.

        A final note on 'ROW_NUMBER' in the SQL query;
        for the `Column.iloc` to work we need to have a rowid column, sqlite3 has it builtin on each table,
        meaning its auto-generated (but the user can overwrite it),
        the issue is that since a VIEW is a virtual-table it doesn't have the rowid column.
        so we are left with one option; select the `rowid` column from the table when creating the view,
        but the issue with that is that the position of the rows no longer corresponds since the amount of rows
        in each table is different,
        so what I did instead is, create a column using the `ROW_NUMBER()` function to get the index/id of each row,
        so it will order the table by rowid, and then create the column using the `ROW_NUMBER` function,
        and finally, alias the new column as '_rowid_' so `self.iloc` can reference it and use it to get rows
        by index position.

        :param expression: Expression
        :return: ColumnView instance
        """
        view_name = f'_col_filtered_{self.table}_{self.name}_{get_random_name(size=10)}_'
        query = f"""
        SELECT ROW_NUMBER() OVER (ORDER BY _rowid_) AS _rowid_, {self.name}
        FROM {self.table} 
        WHERE {expression.query}
        """
        return self._create_and_get_temp_view(view_name=view_name, query=query)

    # def astype(self, py_type) -> ColumnView:
    #     """
    #     Convert/ cast data to given type (if possible)
    #
    #     :param py_type: data-type: str, int, float
    #     :return: ColumnView
    #     """
    #     # sqlite3 data types: https://www.sqlite.org/datatype3.html
    #     valid_types = (str, int, float, bool)
    #     if py_type not in valid_types:
    #         raise ValueError(f'Type must be one of the following: {valid_types}')
    #
    #     py_to_sql_types = {str: 'TEXT', int: 'INTEGER', float: 'REAL', bool: 'BOOLEAN'}
    #     return self._create_and_get_temp_view(
    #         view_name=f'_col_casted_{self.table}_{self.name}_{get_random_name(size=10)}_',
    #         query=f'SELECT CAST({self.name} AS {py_to_sql_types[py_type]}, {ROWID} FROM {self.table}'
    #     )

    def _create_and_get_temp_view(self, view_name: str, query: str) -> ColumnView:
        """
        Create a temporary-view (gets auto deleted at the end of the session) and return a new ColumnView instance

        :param view_name: str
        :param query: str
        :return: ColumnView instance
        """
        if 'AS _rowid_' not in query:
            raise ValueError('Query must alias the rowid column as `_rowid_` for `iloc` to work.')

        create_temp_view(
            conn=self.conn,
            view_name=view_name,
            query=query,
            drop_if_exists=False
        )
        return ColumnView(
            conn=self.conn,
            cache=self._cache,
            table_name=view_name,
            col_name=self.name,
            created_query=query
        )

    def __getitem__(self, item: int | slice | list | Expression) -> Any:
        """
        Return index slice or filtered Column

        You can do two things with Column.__getitem__():
        1. Get value/s at given index
        2. Get a filtered Column

        There are three ways to get a value or list of values at a given index:
        1. pass an integer: db.table.column[28]
        2. passing a slice: db.table.column[8:24:2]
        3. and using a list of indexes: db.table.column[[3, 2, 8, -1, 15]]

        And for filtering a column you can simply pass a column with a logical expression:
        col1 = db.table.col1
        col1[col1 > 10]

        :param item: int | slice | list | Expression | tuple
        :return: IndexLoc | Column
        """
        if isinstance(item, Expression):
            return self.filter(item)
        if isinstance(item, (int, slice, list)):
            return self.iloc[item]

        raise TypeError(f'Argument must be of type Expression, int, slice, or list. not: {type(item)}')

    def __iter__(self) -> Generator[ColumnValue, None, None]:
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
        n = len(self)

        if n <= top_rows:  # shortcut for small dataframes
            return DataFrame(data=self, columns=[self.name])

        data = self.iloc[:top_rows] + self.iloc[-bottom_rows:]
        index = list(range(top_rows)) + list(range(n - bottom_rows, n))
        return DataFrame(index=index, data=data, columns=[self.name])

    def __repr__(self) -> str:
        """ Return column as a Pandas Series """
        return self._repr_df().to_string(show_dimensions=False, max_rows=10)

    def _repr_html_(self) -> str:
        """ Return column in HTML """
        return self._repr_df().to_html(show_dimensions=False, max_rows=10)

    def __add__(self, other: Column | Iterable | Numeric | str) -> Generator[ColumnValue, None, None]:
        """
        Return a generator with the arithmetic operation applied on each element

        :param other: Column | Iterable | int | float | bool | str
        :return: Generator
        """
        if isinstance(other, str) or not isinstance(other, Iterable):
            other = itertools.repeat(other, len(self))

        for x, y in zip(self, other, strict=True):
            if x is None:
                yield x
            else:
                yield x + y

    def __sub__(self, other: Column | Iterable | Numeric) -> Generator[ColumnValue, None, None]:
        """
        Return a generator with the arithmetic operation applied on each element

        :param other: Column | Iterable | float | int
        :return: None
        """
        if not isinstance(other, Iterable):
            other = itertools.repeat(other, len(self))

        for x, y in zip(self, other, strict=True):
            if x is None:
                yield x
            else:
                yield x - y

    def __mul__(self, other: Column | Iterable | Numeric) -> Generator[ColumnValue, None, None]:
        """
        Return a generator with the arithmetic operation applied on each element

        :param other: Column | Iterable | float | int
        :return: None
        """
        if not isinstance(other, Iterable):
            other = itertools.repeat(other, len(self))

        for x, y in zip(self, other, strict=True):
            if x is None:
                yield x
            else:
                yield x * y

    def __truediv__(self, other: Column | Iterable | Numeric) -> Generator[ColumnValue, None, None]:
        """
        Return a generator with the arithmetic operation applied on each element

        :param other: Column | Iterable | float | int
        :return: None
        """
        if not isinstance(other, Iterable):
            other = itertools.repeat(other, len(self))

        for x, y in zip(self, other, strict=True):
            if x is None:
                yield x
            else:
                yield x / y

    def __floordiv__(self, other: Column | Iterable | Numeric) -> Generator[ColumnValue, None, None]:
        """
        Return a generator with the arithmetic operation applied on each element

        :param other: Column | Iterable | float | int
        :return: None
        """
        if not isinstance(other, Iterable):
            other = itertools.repeat(other, len(self))

        for x, y in zip(self, other, strict=True):
            if x is None:
                yield x
            else:
                yield x // y

    def __gt__(self, other: Numeric) -> Expression:
        """

        :param other: float
        :return: Expression
        """
        return Expression(query=f'{self.name} > {convert_type_to_sql(other)}', table=self.table)

    def __ge__(self, other: Numeric) -> Expression:
        """

        :param other: float
        :return: Expression
        """
        return Expression(query=f'{self.name} >= {convert_type_to_sql(other)}', table=self.table)

    def __lt__(self, other: Numeric) -> Expression:
        """

        :param other: float
        :return: Expression
        """
        return Expression(query=f'{self.name} < {convert_type_to_sql(other)}', table=self.table)

    def __le__(self, other: Numeric) -> Expression:
        """

        :param other: float
        :return: Expression
        """
        return Expression(query=f'{self.name} <= {convert_type_to_sql(other)}', table=self.table)

    def __eq__(self, other: ColumnValue) -> Expression:
        """

        :param other: str or float
        :return: Expression
        """
        if other is None:
            return Expression(query=f'{self.name} IS NULL', table=self.table)
        return Expression(query=f'{self.name} = {convert_type_to_sql(other)}', table=self.table)

    def __ne__(self, other: ColumnValue) -> Expression:
        """

        :param other: str or float
        :return: Expression
        """
        if other is None:
            return Expression(query=f'{self.name} IS NOT NULL', table=self.table)
        return Expression(query=f'{self.name} != {convert_type_to_sql(other)}', table=self.table)

    def isin(self, options: Sequence) -> Expression:
        """

        :param options: tuple
        :return: Expression
        """
        options = sql_tuple(options)
        return Expression(query=f'{self.name} IN {options}', table=self.table)

    def between(self, a: Numeric, b: Numeric) -> Expression:
        """

        :param a: float
        :param b: float
        :return: Expression
        """
        return Expression(query=f'{self.name} BETWEEN {convert_type_to_sql(a)} AND {convert_type_to_sql(b)}',
                          table=self.table)

    def like(self, regex: str) -> Expression:
        """

        :param regex: str
        :return: Expression
        """
        return Expression(query=f'{self.name} LIKE {convert_type_to_sql(regex)}', table=self.table)

    # SQLite3 doesn't support ILIKE (LIKE is already case-insensitive)
    # def ilike(self, regex: str) -> Expression:
    #     """
    #
    #     :param regex: str
    #     :return: Expression
    #     """
    #     return Expression(query=f"{self.name} ILIKE '{regex}'", table=self.table)


class ColumnView(Column):
    """
    A ColumnView is created everytime we filter an existing Column.
    (from Column.filter() or Column[<expression>]
    """
    # noinspection PyMissingConstructor
    def __init__(self, conn: sqlite3.Connection, cache: Cache,
                 table_name: str, col_name: str, created_query: str = None) -> None:
        """
        Initialize the Column object

        :param conn: sqlite3.Connection
        :param cache: Cache, instance of Cache
        :param table_name: str
        :param col_name: str
        :param created_query: None | str
        """
        self.conn = conn
        self._cache = cache
        self.table = table_name
        self.name = col_name
        self.query = f'SELECT {col_name} FROM {table_name}'
        self._created_query = created_query  # save the query used in creating the column-view for debugging
