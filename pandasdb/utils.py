import pandas as pd

import sqlite3
from pathlib import Path


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
        conn = sqlite3.connect(':memory:')
        conn.executescript(file.read())
    return conn
