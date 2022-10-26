from __future__ import annotations

from pandas import DataFrame

import sqlite3
from typing import Generator, Callable, Any, Literal

from .exceptions import InvalidColumnError
from .column import Column
from .indexloc import IndexLoc
from .cache import Cache
from .expression import Expression, OrderBy, Limit
from .utils import create_view, get_random_name


class Table:
    """
    An object that represents an SQL table
    """
    def __init__(self, conn: sqlite3.Connection, cache: Cache, name: str) -> None:
        """
        Initialize the Table object

        :param conn: sqlite3.Connection
        :param cache: Cache, instance of Cache
        :param name: str, table name
        """
        self.conn = conn
        self._cache = cache
        self.name = name
        self.query = f'SELECT * FROM {self.name}'

        # TODO store columns in dict, and change self.items() implementation
        for col in self.columns:
            setattr(self, col, Column(conn=self.conn, cache=self._cache, table_name=self.name, col_name=col))

    @property
    def columns(self) -> list[str]:
        """
        Get list with column names
        """
        with self.conn as cursor:
            return [x[1] for x in cursor.execute(f"PRAGMA table_info('{self.name}')")]

    @property
    def len(self) -> int:
        """
        Return amount of rows in the table
        """
        return self._cache.execute(f'SELECT COUNT(*) FROM {self.name}')[0][0]

    @property
    def shape(self) -> tuple:
        """
        Get a tuple with: (n_rows, n_cols)
        """
        return self.len, len(self.columns)

    def describe(self) -> dict[str, dict[str, Any]]:
        """
        Get a nested dictionary with the descriptive properties for each column in the table

        :return: dict, {col1: {'min': 32, 'max': 83 ...}, col2 {'min': 'Alex', 'max': 'Zoey' ...} ...}
        """
        return {name: col.describe() for name, col in self.items()}

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
                return cursor.execute(self.query + f' LIMIT {limit}').fetchall()
            return cursor.execute(self.query).fetchall()

    def sample(self, n: int = 10) -> list[tuple]:
        """
        Get a list of random rows from the table

        :param n: int, number of rows
        :return: list with nested tuples
        """
        with self.conn as cursor:
            return cursor.execute(f'{self.query} ORDER BY RANDOM() LIMIT {n}').fetchall()

    def items(self) -> Generator[tuple[str, Column], None, None]:
        """
        Generator that yields: (column_name, col_object)
        """
        for col in self.columns:
            yield col, getattr(self, col)

    def applymap(self, func: Callable, *, ignore_na: bool = True,
                 args: tuple = tuple(), **kwargs) -> Generator[tuple, None, None]:
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
        :param ignore_na: bool, default: True
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
        1) Singular Integer, ex: iloc[0], iloc[32], or with negative: iloc[-12]
        2) Passing a list of integers, ex: iloc[[1, 22, 4, 3, 17, 38]], iloc[[1, -4, 17, 22, 38, -4, -1]]
        4) Passing Slice, ex: iloc[:10], iloc[2:8], iloc[2:24:2]

        The return type will be a list for multiple items and a tuple for single items

        :return: tuple or list of tuples
        """
        return IndexLoc(obj=self)

    def filter(self, expression: Expression) -> TableView:
        """
        # TODO complete docstring

        :param expression:
        :return:
        """
        view_name = f'_table_{self.name}_{get_random_name(size=10)}_'
        query = f"""
        SELECT 
            ROW_NUMBER() OVER (ORDER BY _rowid_) AS _rowid_, *
        FROM {self.name} 
        WHERE {expression.query}
        """
        create_view(
            conn=self.conn,
            view_name=view_name,
            query=query
        )
        self._cache.views.append(view_name)
        return TableView(conn=self.conn, cache=self._cache, name=view_name)

    # def join(self, table2join: Table | str,  *, on: str = None, left_on: str = None, right_on: str = None, how: str,
    #          select_cols: list = None, suffixes: tuple = ('_self.', '_'), view_name: str = None) -> TableView:
    #     """"""
    #     table2join = Table
    #     if on is not None:
    #         join_statement = f'{on} == {on}'
    #     elif left_on is not None and right_on is not None:
    #         join_statement = f'{left_on} == {right_on}'
    #     else:
    #         raise ValueError("Must provide either 'on' parameter, or 'left_on' and 'right_on'")
    #
    #     select_cols = select_cols or ['*']
    #     view_name = view_name or f'_join_table_{self.name}_{get_random_name(size=10)}_'
    #
    #     query = f"""
    #     SELECT {select_cols} FROM {self.name}
    #     JOIN ON {join_statement}
    #     """
    #     create_view(
    #         conn=self.conn,
    #         view_name=view_name,
    #         query=query
    #     )
    #     self._cache.views.append(view_name)
    #     return TableView(conn=self.conn, cache=self._cache, name=view_name)

    def __iter__(self) -> Generator[tuple, None, None]:
        """
        Yield rows from cursor
        """
        with self.conn as cursor:
            yield from cursor.execute(self.query)

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

    # TODO: add option for list of items/columns and return new Table object with selected columns
    def __getitem__(self, item: str | Expression) -> Column | Table:
        """
        Get column object for given column name

        :param item: str, column-name
        :return: Column
        :raise: KeyError
        """
        if isinstance(item, Expression):
            return self.filter(item)

        elif isinstance(item, str):
            try:
                return self._get_col(item)
            except InvalidColumnError:
                raise KeyError(f'No such Column: {item}, must be one of the following: {", ".join(self.columns)}')

        raise TypeError(f'Argument must be of type str or Expression. not: {type(item)}')

    def __len__(self) -> int:
        """ Return amount of rows """
        return self.len

    def __hash__(self) -> int:
        """ Get hash value of Table """
        return hash(f'{self.name}')

    def _repr_df(self) -> DataFrame:
        """
        Get a sample of the table data
        
        This method is a helper for: __str__, __repr__, and _repr_html_.
        It returns a sample of the table data, by default the first and last 5 rows,...

        note on top_rows and bottom_rows;
        by default pandas display's the first and last 5 rows,
        passing a list than len() > 10 will get the shrink effect, which tells the user
        that there is more data than meets the eye.

        :return: DataFrame
        """
        top_rows = 10
        bottom_rows = 10
        n = len(self)

        if top_rows > n:  # shortcut for small dataframes
            return DataFrame(data=self.iloc[:], columns=self.columns)

        data = self.iloc[:top_rows] + self.iloc[-bottom_rows:]
        index = list(range(top_rows)) + list(range(n - bottom_rows, n))
        return DataFrame(index=index, data=data, columns=self.columns)

    def __repr__(self) -> str:
        """ Return table as a Pandas DataFrame """
        n_rows, n_cols = self.shape
        size_info = f'\n\n[{n_rows} rows x {n_cols} columns]'
        return self._repr_df().to_string(show_dimensions=False, max_rows=10) + size_info

    def _repr_html_(self) -> str:
        """ Return table in HTML """
        return self._repr_df().to_html(show_dimensions=False, max_rows=10)


class TableView(Table):
    """
    A class that represents a View in SQL (or a virtual table in Sqlite3)

    All the methods are the same as a regular table, except the columns' property which will
    filter the list of columns to not return the '_rowid_' column.

    The reason the filtering is necessary is simply because each table in Sqlite3 has a _rowid_ column
    that is hidden (meaning it doesn't appear in 'SELECT * FROM table', only if explicitly queried, for ex:
    'SELECT _rowid_, * FROM table'),
    But virtual tables (views, temporary tables, etc.) don't have a builtin '_rowid_' column,
    so what we do, is we create a column containing each row's index when creating the view,
    but when we do 'SELECT * FROM my_view' we want to filter out the '_rowid_' column,
    so instead we pass the columns' property which will automatically filter it out.
    """
    def __init__(self, conn: sqlite3.Connection, cache: Cache, name: str) -> None:
        """
        Initialize the Table object

        :param conn: sqlite3.Connection
        :param cache: Cache, instance of Cache
        :param name: str, table name
        """
        self.conn = conn
        self._cache = cache
        self.name = name

        # TODO store columns in dict, and change self.items() implementation
        for col in self.columns:
            setattr(self, col, Column(conn=self.conn, cache=self._cache, table_name=self.name, col_name=col))

    @property
    def query(self) -> str:
        """
        Get the "SELECT *" query for the table

        The returned query is a property so if any column is added/ removed, the query will be updated.
        note that the rowid column is filtered out in 'self.columns', you can read more about that
        in the class docstring.
        """
        return f'SELECT {", ".join(self.columns)} FROM {self.name}'

    @property
    def columns(self) -> list[str]:
        """
        Get list with column names

        note that since a table view
        """
        with self.conn as cursor:
            return [x[1] for x in cursor.execute(f"PRAGMA table_info('{self.name}')")
                    if not x[1].startswith('_rowid_')]
