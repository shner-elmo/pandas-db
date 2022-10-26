from __future__ import annotations

from typing import Collection

from . import table, column


BaseTypes = str | int | float | bool | None


class IndexLoc:
    def __init__(self, obj: table.Table | column.Column) -> None:
        self.obj = obj
        self.len = len(obj)  # to avoid recomputing

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

    @staticmethod
    def sql_tuple(it: Collection) -> str:
        """
        Convert an iterable to an SQL-compatible tuple

        if the len(tuple) == 1 then it will remove the comma

        :param it: Collection, object that implements __len__ and __iter__
        :return: str
        """
        return str(tuple(it)).replace(',', '') if len(it) == 1 else str(tuple(it))

    def __getitem__(self, index: int | list | slice) -> tuple | list | BaseTypes:
        """
        Get row/value at given index

        if Table:
            if type(index) == int:
                return tuple
            elif type(index) in [list, slice]:
                return list[tuple]

        elif Column:
            if type(index) == int:
                return BaseTypes #  (str | int | float | bool | None)  # depending on the underlying data in the column
            elif type(index) in [list, slice]:
                return list[BaseTypes]

        :param index: int | list | slice
        :return: tuple | list | BaseTypes
        """
        if isinstance(index, int):
            index = self.index_abs(index)
            self.validate_index(index)
            index += 1

            query = f'{self.obj.query} WHERE _rowid_ == {index}'  # TODO add limit ?
            with self.obj.conn as cursor:
                row = cursor.execute(query).fetchone()
            return row if isinstance(self.obj, table.Table) else row[0]

        if isinstance(index, slice):
            indices = index.indices(self.len)
            indexes = [idx + 1 for idx in range(*indices)]

            query = f'{self.obj.query} WHERE _rowid_ IN {self.sql_tuple(indexes)}'  # LIMIT {len(indexes)}'
            with self.obj.conn as cursor:
                rows = cursor.execute(query)

            return rows.fetchall() if isinstance(self.obj, table.Table) else [tup[0] for tup in rows]

        if isinstance(index, list):
            indexes = [self.index_abs(idx) for idx in index]
            for idx in indexes:
                self.validate_index(idx)
            indexes = [idx + 1 for idx in indexes]

            base_query = self.obj.query.replace("SELECT", "SELECT _rowid_,")
            unique_indexes = set(indexes)
            query = f'{base_query} WHERE _rowid_ IN {self.sql_tuple(unique_indexes)}'  # LIMIT {len(indexes)}'

            with self.obj.conn as cursor:
                rows = cursor.execute(query)

            if isinstance(self.obj, table.Table):
                idx_row_mapping: dict[int, tuple] = {row[0]: row[1:] for row in rows}
            else:
                idx_row_mapping: dict[int, tuple] = dict(rows)

            return [idx_row_mapping[idx] for idx in indexes]

        raise TypeError(f'Index must be of type: int, list, or slice. not: {type(index)}')
