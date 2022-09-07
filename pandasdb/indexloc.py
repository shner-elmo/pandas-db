from typing import Any
from collections.abc import Iterable


class IndexLoc:
    """
    An object for indexing rows/ values in an iterable
    You can pass an int (positive or negative),
    a slice with start, stop, and step, for ex: 3:15 or 3:15:2,
    or you can pass a list, ex: [1, 24, 2, -5, 9, 8, -1]
    """
    def __init__(self, it: Iterable, length: int) -> None:
        """
        Take an iterable and the length for it

        :param it: iterable
        :param length: int
        """
        self.data = it
        self.length = length

    def _validate_idx(self, index: int) -> None:
        """
        Assert given index is above zero, and below or equal to length,
        else: raise IndexError

        :param index: int, must be positive
        :raise IndexError: if not 0 <= index < length
        :return: None
        """
        if not 0 <= index < self.length:
            raise IndexError('Given index out of range')

    def __getitem__(self, key: int | list | slice):  # -> list | tuple | str | int | float
        """
        Get data by: index, list, or slice

        Getitem supports three ways of indexing the iterable:
        1) Singular Integer, ex: IndexIloc[0], IndexIloc[32], or with negative: IndexIloc[-12]
        2) Passing a list of integers, ex: IndexIloc[[1, 22, 4, 3, 17, 38]], IndexIloc[[1, -4, 17, 22, 38, -4, -1]]
        4) Passing Slice, ex: IndexIloc[:10], IndexIloc[2:8], IndexIloc[2:24:2]

        The return type depends on if the underlying object is a Table or Column,
        and if the requested data is one or multiple items;

        if Table:
            if type(index) == int:
                return tuple
            elif type(index) in [list, slice]:
                return list[tuple]

        elif Column:
            if type(index) == int:
                return str | int | float  # depending on the type of the data for the column
            elif type(index) in [list, slice]:
                return list

        :param key: int, list, or slice
        :return:
        """
        # TODO: get data directly from through SQL and use ORDER BY x DESC for negative indexes
        if not isinstance(key, (int, list, slice)):
            raise TypeError(f'Index must be of type: int, list, or slice, not: {type(key)}')

        if isinstance(key, int):
            if key < 0:
                key = self.length + key

            self._validate_idx(index=key)
            for idx, val in enumerate(self.data):
                if idx == key:
                    return val

        if isinstance(key, (list, slice)):
            keys = key  # plural for readability
            if isinstance(keys, slice):
                indices = keys.indices(self.length)
                keys = range(*indices)

            new_keys = []
            # validate all indexes and convert negatives to positive
            for idx in keys:
                if idx < 0:
                    idx = self.length + idx

                self._validate_idx(index=idx)
                new_keys.append(idx)

            keys = new_keys
            items: list[tuple[int, Any]] = []

            for idx, val in enumerate(self.data):
                if idx in keys:
                    for i in range(keys.count(idx)):
                        items.append((idx, val))

                    if len(items) == len(keys):
                        break

            items_dict = dict(items)
            return [items_dict[x] for x in keys]
