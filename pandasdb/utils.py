from __future__ import annotations

import pandas as pd
from pympler import asizeof

import random
import string
import sqlite3
from pathlib import Path
from typing import Generator, Iterable, Any, TypeVar, Protocol

PrimitiveTypes = str | int | float | bool | None
T = TypeVar("T")
TypeAny = TypeVar('TypeAny', bound=Any)


class SizedIterable(Protocol):
    def __len__(self) -> int:
        ...

    def __iter__(self) -> SizedIterable:
        ...

    def __next__(self) -> Any:
        ...

# Unfortunately Pycharm doesn't support Protocol classes, so use Collection instead (for indexloc.sql_tuple)
# https://stackoverflow.com/a/49434182/18042558


def convert_type_to_sql(x: str | int | float | bool) -> str:
    """
    Function that takes a value (primitive type) and returns an SQL-compatible string

    if type is str -> "'x'"
    if type is int or float -> '3' | '3.2'
    if type is bool -> 'true' | 'false'

    :param x: str | int | float | bool
    :return: str
    """
    if isinstance(x, str):
        return f"'{x}'"
    if isinstance(x, bool):  # above int because bool inherits from int
        return str(x).lower()
    if isinstance(x, (int, float)):
        return str(x)

    raise TypeError(f'param x must be of type str, int, float, or bool. not: {type(x)}')


def sql_tuple(it: Iterable) -> str:
    """
    Convert an iterable to an SQL-compatible tuple

    :param it: Iterable
    :return: str
    """
    return f'({", ".join(convert_type_to_sql(x) for x in it)})'


def sqlite_conn_open(conn: sqlite3.Connection) -> bool:
    """
    Return True if connection is open, else: False

    :param conn: sqlite3.Connection
    :return: bool
    """
    try:
        conn.cursor()
        return True
    except sqlite3.ProgrammingError:
        return False


def get_random_name(size: int = 10) -> str:
    """
    Get a string with random letters (from string.ascii_lowercase)

    :param size: int, default 10
    :return: str
    """
    return ''.join(random.choices(string.ascii_lowercase, k=size))


# TODO move to connection.Database
def create_view(conn: sqlite3.Connection, view_name: str, query: str, drop_if_exists: bool = False) -> None:
    """
    Create view from given sql query

    :param conn: sqlite3 connection
    :param view_name: str
    :param query: str, select query
    :param drop_if_exists: bool, default False
    :raises: ValueError if view_name already exists
    :return: None
    """
    with conn as cursor:
        views: Iterable[str] = (x[0] for x in cursor.execute("SELECT name FROM sqlite_master WHERE type='view'"))

    if view_name in views:
        if drop_if_exists:
            with conn as cursor:
                cursor.execute(f'DROP VIEW {view_name}')
        else:
            raise ValueError(f"view '{view_name}' already exists")

    with conn as cursor:
        cursor.execute(f"CREATE VIEW {view_name} AS {query}")


def same_val_generator(val: TypeAny, size: int) -> Generator[TypeAny, None, None]:
    """ Generator that yield a given value n amount of times """
    for _ in range(size):
        yield val


def infinite_generator(val: TypeAny) -> Generator[TypeAny, None, None]:
    """ Generator the yields a given value infinitely """
    while True:
        yield val


def concat(*args: str | Iterable, sep: str = '') -> Generator:
    """
    Return a generator with the elements concatenated

    You can pass both strings and Iterables (list, tuple, set, dict, generator, etc..)
    if you pass an iterable than the length must be the same as the length of the column

    Example:
    it = concat(db.table.first_name, '-', db.table.last_name)
    print(next(it))
    # out: 'Jake-Roberts'

    :param args: str | Iterable
    :param sep: str, default: ''
    :return: Generator
    """
    converted_args = []
    for arg in args:
        if isinstance(arg, str) or not isinstance(arg, Iterable):
            arg = infinite_generator(arg)
        converted_args.append(arg)

    for tup in zip(*converted_args):
        stringify_tup = map(str, tup)
        concat_tup = sep.join(stringify_tup)
        yield concat_tup


def get_mb_size(*obj) -> float:
    """
    A helper for getting the number of Megabytes an object/s is taking in memory

    :param obj: args, any object/s
    :return: float
    """
    bytes_size = asizeof.asizeof(*obj)
    return bytes_size / 1e+6


def rename_duplicate_cols(columns: list) -> list:
    """
    for each duplicated column it will add a number as the suffix

    ex: ['a', 'b', 'c', 'a', 'b', 'b'] -> ['a', 'b', 'c', 'a_2', 'b_2', 'b_3']

    :param columns: DataFrame
    :return: list
    """
    new_cols = []
    prev_cols = []  # previously iterated columns in for loop

    for col in columns:
        prev_cols.append(col)
        count = prev_cols.count(col)

        if count > 1:
            new_cols.append(f'{col}_{count}')
        else:
            new_cols.append(col)
    return new_cols


def convert_db_to_sql(db_file: str, sql_file: str) -> None:
    """
    takes a .db file and converts it to .sql

    :param db_file: str, path/name to save new .db file
    :param sql_file: str, path to .sql file
    :return:
    """
    with sqlite3.connect(db_file) as conn:
        with open(sql_file, 'w') as file:
            for line in conn.iterdump():
                file.write(line)


def convert_csvs_to_db(db_file: str, csv_files: list, set_lowercase: bool = True, **kwargs: Any) -> None:
    """
    convert a CSV list to a database (.db file)

    if the param set_lowercase is true, it will rename the table and column names to lowercase
    note that any column name that contains spaces or dashes will replace the characters with underscores,
    for ex: 'first name' -> 'first_name'

    :param db_file: str, path/name to save new .db file
    :param csv_files: list, ex: ['orders.csv', 'names.csv', 'regions.csv'...]
    :param set_lowercase: bool, default True
    :param kwargs: key-word arguments to pass to pd.read_csv()
    :return: None
    """
    with sqlite3.connect(db_file) as conn:

        for csv in csv_files:
            df = pd.read_csv(csv, **kwargs)
            df.columns = df.columns.str.replace(' ', '_').str.replace('-', '_')

            if set_lowercase:
                df.columns = df.columns.str.lower()

            name = Path(csv).stem.replace(' ', '_').replace('-', '_')
            df.to_sql(name=name, con=conn, index=False)


def convert_sql_to_db(sql_file: str, db_file: str) -> None:
    """
    Convert .sql to .db file

    :param sql_file: str, path to .sql file
    :param db_file: str, path/name to save new .db file
    :return: None
    """
    with open(sql_file, 'r') as file:
        with sqlite3.connect(db_file) as conn:
            conn.executescript(file.read())


def load_sql_to_sqlite(sql_file: str) -> sqlite3.Connection:
    """
    Create a Sqlite3 connection with a .sql file

    :param sql_file: str, path to .sql file
    :return: sqlite3 connection
    """
    with open(sql_file, 'r') as file:
        conn = sqlite3.connect(':memory:', check_same_thread=False)
        conn.executescript(file.read())
        return conn
