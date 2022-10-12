from __future__ import annotations

import pandas as pd
from pympler import asizeof

import sqlite3
from pathlib import Path
from typing import Generator, Iterable, Any, TypeVar, Protocol

BaseTypes = str | int | float
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
    conn = sqlite3.connect(db_file)
    with open(sql_file, 'w') as file:
        for line in conn.iterdump():
            file.write(line)
    conn.close()


def convert_csvs_to_db(db_file: str, csv_files: list) -> None:
    """
    convert a CSV list to a database (.db file)

    :param db_file: str, path/name to save new .db file
    :param csv_files: list, ex: ['orders.csv', 'names.csv', 'regions.csv'...]
    :return: None
    """
    conn = sqlite3.connect(db_file)

    for csv in csv_files:
        df = pd.read_csv(csv)
        name = Path(csv).stem
        df.to_sql(name=name, con=conn, index=False)
    conn.close()


def convert_sql_to_db(sql_file: str, db_file: str) -> None:
    """
    Convert .sql to .db file

    :param sql_file: str, path to .sql file
    :param db_file: str, path/name to save new .db file
    :return: None
    """
    with open(sql_file, 'r') as file:
        conn = sqlite3.connect(db_file)
        conn.executescript(file.read())
    conn.close()


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
