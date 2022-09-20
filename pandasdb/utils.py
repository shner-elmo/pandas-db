import pandas as pd
from pympler import asizeof

import sqlite3
from pathlib import Path
from typing import Any

# from .column import Column


def mb_size(*obj) -> float:
    """
    A helper for getting the number of Megabytes an object/s is taking in memory

    :param obj: args, any object/s
    :return: float
    """
    bytes_size = asizeof.asizeof(*obj)
    return bytes_size / 1e+6


def validate_column_is_numeric(col1: 'Column', col2: 'Column') -> None:
    """
    Validate columns data is numeric (type must be either int or float)

    :param col1: Column
    :param col2: Column
    :raises ValueError if not both cols numeric
    :return: None
    """
    if not col1.data_is_numeric() and col2.data_is_numeric():
        raise ValueError(f'Both columns data must be numeric (int or float), not {col1.type} and {col2.type}')


def validate_data_is_numeric(x: Any) -> None:
    """
    Validate data is numeric (type must be either int or float)

    :param x: Any
    :raises ValueError if type(x) not in [int, float]
    :return: None
    """
    if not isinstance(x, (int, float)):
        raise ValueError(f'Value must be numeric: str, int, float, or Column, not {type(x)}')


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
    convert a list of CSV's to a DataBase (.db file)

    :param db_file: str, path/name to save new .db file
    :param csv_files: list, ex: ['orders.csv', 'names.csv', 'regions.csv'...]
    :return:
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
