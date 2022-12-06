from __future__ import annotations

from pandas import DataFrame

import sqlite3
from typing import Generator, Callable, Any, overload, Literal

from .exceptions import InvalidColumnError
from .column import Column
from .cache import Cache
from .expression import Expression
from .utils import create_temp_view, get_random_name, sql_tuple

PrimitiveTypes = str | int | float | bool | None
TableRow = tuple[PrimitiveTypes, ...]
ROWID = '_rowid_ AS _rowid_'  # select rowid/oid/_rowid_ as _rowid_


class IndexLoc:
    def __init__(self, table: Table) -> None:
        self.table = table
        self.len = len(table)  # to avoid recomputing

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
    def __getitem__(self, index: int) -> TableRow:
        ...

    @overload
    def __getitem__(self, index: slice | list) -> list[TableRow]:
        ...

    def __getitem__(self, index: int | slice | list) -> TableRow | list[TableRow]:
        """
        Get row/value at given index

        if an int is passed; then values at the given index will be returned
        if a slice or a list is passed; then it will return a list of elements/cells from the table

        Note that the index is always increased by one before being passed to the SQL query,
        as the rowid columns starts the count at 1 and not 0.

        :param index: int | slice | list
        :return: tuple | list | BaseTypes
        """
        def select_cols_str() -> str:
            return ', '.join(self.table.columns)

        if isinstance(index, int):
            index = self.index_abs(index)
            self.validate_index(index)
            index += 1

            with self.table.conn as cursor:
                query = f'SELECT {select_cols_str()} FROM {self.table.name} WHERE _rowid_ == {index}'
                return cursor.execute(query).fetchall()[0]

        if isinstance(index, slice):
            indices = index.indices(self.len)
            indexes = [idx + 1 for idx in range(*indices)]

            with self.table.conn as cursor:
                query = f'SELECT {select_cols_str()} FROM {self.table.name} WHERE _rowid_ IN {sql_tuple(indexes)}'
                return cursor.execute(query).fetchall()

        if isinstance(index, list):
            indexes = [self.index_abs(idx) for idx in index]
            for idx in indexes:
                self.validate_index(idx)
            indexes = [idx + 1 for idx in indexes]
            unique_indexes = set(indexes)

            with self.table.conn as cursor:
                rows = cursor.execute(
                    f'SELECT _rowid_, {select_cols_str()} FROM {self.table.name} '
                    f'WHERE _rowid_ IN {sql_tuple(unique_indexes)}')

            idx_row_mapping: dict[int, TableRow] = {tup[0]: tup[1:] for tup in rows}  # first item in tuple is _rowid_
            return [idx_row_mapping[idx] for idx in indexes]

        raise TypeError(f'Index must be of type: int, list, or slice. not: {type(index)}')


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
        self.name = name
        self._cache = cache
        self._column_items: dict[str, Column] = {}

        for column in self.columns:
            self._set_column(column)

    @property
    def query(self) -> str:
        """
        Get the "SELECT *" query for the table

        The returned query is a property so if any column is added/ removed, the query will be updated.
        note that the rowid column is filtered out in 'self.columns', you can read more about that
        in the class docstring.
        """
        return f'SELECT {self._cols_as_str} FROM {self.name}'

    @property
    def columns(self) -> list[str]:
        """
        Get list with column names
        """
        with self.conn as cursor:
            return [x[1] for x in cursor.execute(f"PRAGMA table_info('{self.name}')")]

    @property
    def _cols_as_str(self) -> str:
        """
        Get a string of columns that can be passed directly to an SQL query
        """
        return ', '.join(self.columns)

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

        :return: dict, {col1: {'min': 32, 'max': 83 ...}, col2: {'min': 'Alex', 'max': 'Zoey' ...} ...}
        """
        return {name: col.describe() for name, col in self.items()}

    def to_df(self) -> DataFrame:
        """
        Return table as a Pandas DataFrame
        """
        return DataFrame(data=iter(self), columns=self.columns)

    def data(self, limit: int = None) -> list[TableRow]:
        """
        Get table data in a nested list, ex: [('AMD', 78.54, True), ('AAPL', 125.34, True)...]

        :param limit: int
        :return: list
        """
        with self.conn as cursor:
            if limit:
                return cursor.execute(self.query + f' LIMIT {limit}').fetchall()
            return cursor.execute(self.query).fetchall()

    def sample(self, n: int = 10) -> list[TableRow]:
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
        yield from self._column_items.items()

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
        return IndexLoc(self)

    def filter(self, expression: Expression) -> TableView:
        """
        Filter and return a table

        The way this works is you have to pass an expression, which you could either type manually:
        `from pandasdb import Expression`
        `df = db.table`
        `df.filter(Expression('col BETWEEN 3.2 AND 7.8', table_name))`

        Or by performing a logical operation with a column (just like in Pandas and Numpy)
        `df = db.table`
        `df.filter((df.col > 3.2) & (df.col < 7.8))`
        Or with `Column.between()`:
        `df.filter(df.col.between(3.2, 7.8))`

        And there is a shortcut for .filter() using square brackets:
        `df[df.col.between(3.2, 7.8)]`

        - How it works:
        Whenever you pass an expression, i.e.: `df.col == 'Jack'` a SQL query will be generated with the following:
        `SELECT * FROM table WHERE col == 'Jack'` and it will create an SQL-view to store the filtered data,
        the view itself can be filtered again and again.

        :param expression: Expression
        :return: TableView
        """
        view_name = f'_table_{self.name}_{get_random_name(size=10)}_'
        query = f"""
        SELECT ROW_NUMBER() OVER (ORDER BY _rowid_) AS _rowid_, {self._cols_as_str}
        FROM {self.name} 
        WHERE {expression.query}
        """
        return self._create_and_get_temp_view(view_name=view_name, query=query)

    def sort_values(self, column: str | list[str] | dict[str, Literal['ASC', 'DESC']],
                    ascending: bool = True) -> TableView:
        """
        Return a new table with the sorted data

        You can pass:
        1) A column-name with the optional parameter ascending
        2) A list of column names, which will all be ordered ascending
        3) A dictionary with the column-name and the sorting order, for ex:
        {'col1': 'ASC', 'col2': 'DESC', 'col3': 'ASC'}

        :param column: str | list | dict
        :param ascending: bool, default True
        """
        if isinstance(column, str):
            order_by_query = f'{column} {"ASC" if ascending else "DESC"}'

        elif isinstance(column, list):
            order_by_query = f'{", ".join(column)}'

        elif isinstance(column, dict):
            cols = [f'{col} {sort_order}' for col, sort_order in column.items()]
            order_by_query = f'{", ".join(cols)}'

        else:
            raise TypeError(f'column parameter must be str, list, or dict, not: {type(column)}')

        view_name = f'_table_sorted_{self.name}_{get_random_name(size=10)}_'
        query = f"""
        SELECT ROW_NUMBER() OVER (ORDER BY {order_by_query}) AS _rowid_, {self._cols_as_str}
        FROM {self.name} 
        """
        return self._create_and_get_temp_view(view_name=view_name, query=query)

    def limit(self, n: int) -> TableView:
        """
        Return a new Table with n amount of rows

        :param n: int
        :return: TableView
        """
        view_name = f'_table_limit_{self.name}_{get_random_name(size=10)}_'
        query = f'SELECT {ROWID}, {self._cols_as_str} FROM {self.name} WHERE _rowid_ <= {n}'
        return self._create_and_get_temp_view(view_name=view_name, query=query)

    def _create_and_get_temp_view(self, view_name: str, query: str) -> TableView:
        """
        Create a temporary-view (gets auto deleted at the end of the session) and return a new TableView instance

        :param view_name: str
        :param query: str
        :return: TableView instance
        """
        if 'AS _rowid_' not in query:
            raise ValueError('Query must alias the rowid column as `_rowid_` for `iloc` to work.')

        create_temp_view(
            conn=self.conn,
            view_name=view_name,
            query=query,
            drop_if_exists=False
        )
        return TableView(
            conn=self.conn,
            cache=self._cache,
            name=view_name,
            created_query=query
        )

    def __iter__(self) -> Generator[TableRow, None, None]:
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
            raise InvalidColumnError(f'Column must be one of the following: {self._cols_as_str}')
        return getattr(self, column)

    def _set_column(self, column: str) -> None:
        """
        Create and cache column object

        :param column: str
        :return: None
        """
        col_obj = Column(conn=self.conn, cache=self._cache, table_name=self.name, col_name=column)
        self._column_items[column] = col_obj

        if not hasattr(self, column):  # to avoid overwriting existing attributes and methods
            setattr(self, column, col_obj)

    def _column_slice(self, columns: list[str]) -> TableView:
        """
        Return a TableView with the given columns

        :param columns: list of column names
        :return: TableView
        """
        view_name = f'_table_{self.name}_{get_random_name(size=10)}_'
        query = f'SELECT {ROWID}, {", ".join(columns)} FROM {self.name}'
        return self._create_and_get_temp_view(view_name=view_name, query=query)

    @overload
    def __getitem__(self, item: Expression) -> TableView:
        ...

    @overload
    def __getitem__(self, item: str) -> Column:
        ...

    @overload
    def __getitem__(self, item: list[str]) -> TableView:
        ...

    def __getitem__(self, item: Expression | str | list[str]) -> Column | TableView:
        """
        Get column object for given column name

        :param item: Expression, column-name, or list of columns
        :return: Column
        :raise: KeyError
        """
        if isinstance(item, Expression):
            return self.filter(item)

        if isinstance(item, str):
            try:
                return self._get_col(item)
            except InvalidColumnError:
                raise KeyError(f'No such Column: {item}, must be one of the following: {self._cols_as_str}')

        if isinstance(item, list):
            return self._column_slice(item)

        raise TypeError(f'Argument must be of type str or Expression. not: {type(item)}')

    def __getattribute__(self, item) -> Any:
        """ Get attribute """
        # for avoiding 'Unresolved attribute' warnings (in Pycharm), this somehow fixes it
        return super().__getattribute__(item)

    def __len__(self) -> int:
        """ Return amount of rows """
        return self.len

    def __hash__(self) -> int:
        """ Get hash value of Table """
        return hash(f'{self.name}')

    def _repr_df(self) -> DataFrame:
        """
        Get a sample of the table data
        
        This method is a helper for: __repr__(), and _repr_html_().
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

        if n <= top_rows:  # shortcut for small dataframes
            return DataFrame(data=self, columns=self.columns)

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

    def equals(self, other) -> bool:
        """
        Check if the content of a given Table is the same
        """
        if not isinstance(other, Table):
            return False

        if self.shape != other.shape:
            return False

        if id(self) == id(other):  # shortcut for same object from another variable
            return True

        if any(a != b for a, b in zip(self, other, strict=True)):
            return False
        return True


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
    # noinspection PyMissingConstructor
    def __init__(self, conn: sqlite3.Connection, cache: Cache, name: str, created_query: str = None) -> None:
        """
        Initialize the Table object

        :param conn: sqlite3.Connection
        :param cache: Cache, instance of Cache
        :param name: str, table name
        :param created_query: str | None
        """
        self.conn = conn
        self._cache = cache
        self.name = name
        self._column_items: dict[str, Column] = {}
        self._created_query = created_query  # save the query used in creating the table-view for debugging

        for column in self.columns:
            self._set_column(column)

    @property
    def columns(self) -> list[str]:
        """
        Get list with column names

        note that since a table view
        """
        with self.conn as cursor:
            return [x[1] for x in cursor.execute(f"PRAGMA table_info('{self.name}')")
                    if not x[1].startswith('_rowid_')]
