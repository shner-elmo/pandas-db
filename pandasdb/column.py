from __future__ import annotations

from pandas import DataFrame, Series

import sqlite3
from typing import Generator, Callable, Any, Sequence, TypeVar, Iterable

from .expression import Expression, OrderBy, Limit
from .indexloc import IndexLoc
from .cache import Cache
from .utils import same_val_generator, create_view, get_random_name, sql_tuple, convert_type_to_sql

PrimitiveTypes = str | int | float | bool | None
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

    # @classmethod
    # def create_column_from_query(cls, conn: sqlite3.Connection, cache: Cache, table_name: str, col_name: str,
    #                              query: str) -> Column:
    #     """
    #     Return an instance of Column with a custom query
    #
    #     :param conn: sqlite3.Connection
    #     :param cache: Cache, instance of Cache
    #     :param table_name: str
    #     :param col_name: str
    #     :param query: str, SQL query
    #     :return: Column instance
    #     """
    #     column = cls(conn=conn, cache=cache, table_name=table_name, col_name=col_name)
    #     column.query = query
    #     return column

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

        col = self.filter(self.__ne__(None), self.sort_values())  # filter None values and sort column
        n = len(col)
        mid = n // 2 - 1  # remove one since index starts at zero

        if n % 2 == 0:
            lst: list[Numeric] = col.iloc[[mid, mid + 1]]
            return sum(lst) / 2
        else:
            return col.iloc[mid]

    def mode(self) -> dict[PrimitiveTypes, int]:
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

    def unique(self) -> list[PrimitiveTypes]:
        """
        Get list with unique values

        :return list
        """
        return list(tup[0] for tup in self._cache.execute(f'SELECT DISTINCT {self.name} FROM {self.table}'))

    def value_counts(self) -> dict[PrimitiveTypes, int]:
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

    def data(self, limit: int = None) -> list[PrimitiveTypes]:
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

    def sample(self, n: int = 10) -> list[PrimitiveTypes]:
        """
        Get a list of random values from the column

        :param n: int, number of values
        :return: list
        """
        with self.conn as cursor:
            return [tup[0] for tup in cursor.execute(f'{self.query} ORDER BY RANDOM() LIMIT {n}')]

    def apply(self, func: Callable[[PrimitiveTypes, ...], T], *, ignore_na: bool = True,
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

    def filter(self, expression: Expression = None, order_by: OrderBy = None, limit: Limit = None) -> Column:
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

        the view name will start with '_col_` since the data within the table-view represents a column,
        and we add 10 random letters to the name, so we can create other filters for the same column without
        having to delete previous ones, or overwrite them.

        note that the view name is store in a list (in self._cache.views) so when the user closes the connection
        with `db.exit()`, all the views in the list will be dropped/ deleted.

        A final note on 'ROW_NUMBER' in the SQL query;
        for the `Column.iloc` to work we need to have a rowid column, sqlite3 has it builtin on each table,
        meaning its auto-generated (but the user can overwrite it),
        the issue is that since a VIEW isn't a table it doesn't have the rowid column.
        so we are left with one option; select the `rowid` column from the table when creating the view,
        but the issue with that is that the position of the rows no longer corresponds since the amount of rows
        in each table is different,
        so what I did instead is, create a column using the `ROW_NUMBER` function to get the index/id of each row,
        and since I need to pass a column, I passed `rowid`.
        So it will order the table by rowid, and then create the column using the `ROW_NUMBER` function,
        and finally, alias the new column as 'rowid' so `self.iloc` can reference it and use it to get rows
        by index position.

        # TODO add docu for orderby and limit parameters
        :param expression: Expression
        :param order_by: OrderBy
        :param limit: Limit
        :return: Column instance
        """
        view_name = f'_col_{self.table}_{self.name}_{get_random_name(size=10)}_'

        if order_by:
            # shortcut for when order_by is not None, when using an order by statement,
            # the _rowid_ needs to be reordered for the indexes to match the rows
            query = f"""
            SELECT 
                ROW_NUMBER() OVER (ORDER BY {order_by.cols}) AS _rowid_, {self.name} 
            FROM (SELECT _rowid_, {self.name} FROM {self.table}
                {f"WHERE {expression.query}" if expression else ""}
                {f"LIMIT {limit.limit}" if limit else ""})
            """
        else:
            query = f"""
            SELECT 
                ROW_NUMBER() OVER (ORDER BY _rowid_) AS _rowid_,
                {self.name}
            FROM {self.table} 
            """
            if expression:
                query += f' WHERE {expression.query}'
            if limit:
                query += f' LIMIT {limit.limit}'

        create_view(
            conn=self.conn,
            view_name=view_name,
            query=query
        )
        # --------------------------------------------
        self._cache.views.append(view_name)  # save name to delete the SQL VIEW just before closing the connection
        return ColumnView(
            created_query=query,
            conn=self.conn,
            cache=self._cache,
            table_name=view_name,
            col_name=self.name
        )

    def __getitem__(self, item: int | slice | list | Expression | tuple) -> Any:
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

        You can optionally pass a OrderBy object, and/or a Limit object:
        # TODO complete

        :param item: int | slice | list | Expression | tuple
        :return: IndexLoc | Column
        """
        if isinstance(item, Expression):
            return self.filter(item)
        elif isinstance(item, tuple):
            return self.filter(*item)
        elif isinstance(item, (int, slice, list)):
            return self.iloc[item]

        raise TypeError(f'Argument must be of type Expression, int, slice, or list. not: {type(item)}')

    def __iter__(self) -> Generator[PrimitiveTypes, None, None]:
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

        if top_rows > n:  # shortcut for small dataframes
            return DataFrame(data=self.iloc[:], columns=[self.name])

        data = self.iloc[:top_rows] + self.iloc[-bottom_rows:]
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

    def __eq__(self, other: PrimitiveTypes) -> Expression:
        """

        :param other: str or float
        :return: Expression
        """
        if other is None:
            return Expression(query=f'{self.name} IS NULL ', table=self.table)
        return Expression(query=f'{self.name} = {convert_type_to_sql(other)} ', table=self.table)

    def __ne__(self, other: PrimitiveTypes) -> Expression:
        """

        :param other: str or float
        :return: Expression
        """
        if other is None:
            return Expression(query=f'{self.name} IS NOT NULL ', table=self.table)
        return Expression(query=f'{self.name} != {convert_type_to_sql(other)} ', table=self.table)

    def isin(self, options: Sequence) -> Expression:
        """

        :param options: tuple
        :return: Expression
        """
        options = sql_tuple(options)
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

    def not_null(self) -> Expression:
        """
        Return an Expression object that filters NULL values the column ('SELECT col FROM table WHERE col IS NOT NULL')

        :return: Expression instance
        """
        return Expression(query=f'{self.name} IS NOT NULL', table=self.table)

    def sort_values(self, ascending: bool = True) -> OrderBy:
        """
        Return a OrderBy object (which can be passed to column.filter())

        :param ascending: bool, default True
        :return: OrderBy instance
        """
        return OrderBy(column=self.name, ascending=ascending)

    @staticmethod
    def limit(limit: int) -> Limit:
        """
        Return a Limit object that limits the amount of rows in a column

        (creates a view with: "SELECT ... LIMIT {limit})

        :param limit: int
        :return: Limit instance
        """
        return Limit(limit=limit)


class ColumnView(Column):
    """
    A ColumnView is created everytime we filter an existing Column.
    (from Column.filter() or Column[<expression>]
    """

    def __init__(self, created_query: str, conn: sqlite3.Connection, cache: Cache,
                 table_name: str, col_name: str) -> None:
        """
        Initialize the Column object

        :param created_query: str
        :param conn: sqlite3.Connection
        :param cache: Cache, instance of Cache
        :param table_name: str
        :param col_name: str
        """
        self._created_query = created_query  # save the query used in creating the column-view for debugging
        self.conn = conn
        self._cache = cache  # TODO move column objects to dict and make attributes public
        self.table = table_name
        self.name = col_name
        self.query = f'SELECT {col_name} FROM {table_name}'
