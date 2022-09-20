from pandas import DataFrame

import sqlite3
from typing import Generator, Callable, Any

from .exceptions import InvalidColumnError
from .column import Column
from .indexloc import IndexLoc
from .cache import Cache


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
        self._name = name
        self._query = f'SELECT * FROM {self._name}'

        for col in self.columns:
            setattr(self, col, Column(conn=self.conn, cache=self._cache, table_name=self._name, col_name=col))

    @property
    def columns(self) -> list[str]:
        """
        Get list with column names
        """
        return [x[1] for x in self._cache.execute(f"PRAGMA table_info('{self._name}')")]

    @property
    def len(self) -> int:
        """
        Return amount of rows in the table
        """
        return self._cache.execute(f'SELECT COUNT(*) FROM {self._name}')[0][0]

    @property
    def shape(self) -> tuple:
        """
        Get a tuple with: (n_rows, n_cols)
        """
        return self.len, len(next(iter(self)))

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
                return cursor.execute(self._query + f' LIMIT {limit}').fetchall()
            return cursor.execute(self._query).fetchall()

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
        1) Singular Integer, ex: IndexIloc[0], IndexIloc[32], or with negative: IndexIloc[-12]
        2) Passing a list of integers, ex: IndexIloc[[1, 22, 4, 3, 17, 38]], IndexIloc[[1, -4, 17, 22, 38, -4, -1]]
        4) Passing Slice, ex: IndexIloc[:10], IndexIloc[2:8], IndexIloc[2:24:2]

        The return type will be a list for multiple items and a tuple for single items

        :return: tuple or list of tuples
        """
        return IndexLoc(it=iter(self), length=len(self))

    def __iter__(self) -> Generator[tuple, None, None]:
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

    def _repr_df(self) -> DataFrame:
        """
        Get a sample of the table data
        
        This method is a helper for: __str__, __repr__, and _repr_html_.
        It returns a sample of the table data, by default: 10 rows and 5 columns,
        and adds an index column with the correspondent index to get the same rows via Table.iloc[].
        The returned Dataframe may appear like it has more than 10 rows but in reality

        :return: DataFrame
        """
        max_rows = 10
        max_cols = 5
        n_rows, n_cols = self.shape

        index_col = ['__index_col__']
        if n_rows > max_rows:
            index_col.extend(range(max_rows // 2))
            index_col.append('...')
            index_col.extend(range(n_rows - (max_rows // 2), n_rows))
        else:
            index_col.extend(range(n_rows))

        cols = [index_col]

        for idx, items in enumerate(self.items()):
            name: str = items[0]
            column: Column = items[1]

            if idx == max_cols:
                break

            column_data = [name]
            if n_rows > max_rows:
                column_data.extend(column.iloc[:max_rows // 2])
                column_data.append('...')
                column_data.extend(column.iloc[-max_rows // 2:])
            else:
                column_data.extend(column.iloc[:])

            cols.append(column_data)

        if len(cols) - 1 < n_cols:  # columns - index
            idx = n_cols // 2
            empty_col = ['...'] * len(cols[0])
            cols.insert(idx, empty_col)

        data = list(zip(*cols))  # transpose nested list; [(col1), (col2)] -> [(row1), (row2), (row3)...]
        df = DataFrame(data=data[1:], columns=data[0])
        df = df.set_index('__index_col__')
        df.index.name = None
        return df

    def __str__(self) -> str:
        """ Return table as a Pandas DataFrame """
        return self._repr_df().to_string(show_dimensions=False)

    def __repr__(self) -> str:
        """ Return table as a Pandas DataFrame """
        return self._repr_df().to_string(show_dimensions=False)

    def _repr_html_(self) -> str:
        """ Return table in HTML """
        return self._repr_df().to_html(show_dimensions=False)
