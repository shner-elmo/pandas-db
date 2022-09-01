from typing import Any
from collections import Counter


class IndexLoc:
    """
    An object for indexing rows/ values in an iterable
    You can pass an int (positive or negative),
    a slice with start, stop, and step, for ex: 3:15 or 3:15:2,
    or you can pass a list, ex: [1, 24, 2, -5, 9, 8, -1]
    """

    def __init__(self, it: iter, length: int) -> None:  # iter correct type hint?
        """
        # TODO finish docs
        :param it: iterable
        :param length: int
        """
        self.data = it
        self.length = length

    def __getitem__(self, key):  # -> list | tuple | or: str, int, float
        """
        Getitem supports three ways of indexing the iterable:
        1) Singular Integer, ex: IndexIloc[0], IndexIloc[32], or with negative: IndexIloc[-12]
        2) Passing Slice, ex: IndexIloc[:10], IndexIloc[2:8], IndexIloc[2:24:2]
        3) Passing a list of integers, ex: IndexIloc[[1, 22, 4, 3, 17, 38]], IndexIloc[[1, -4, 17, 22, 38, -4, -1]]

        :param key:
        :return: TODO finish docs
        """
        def validate_idx(index: int, length: int) -> None:
            """
            Assert given index is above zero, and below or equal to length,
            else: raise IndexError

            :param index: int, must be positive
            :param length: IndexError if 0 <= index < length
            :return:
            """
            if not 0 <= index < length:
                raise IndexError('Given index out of range')

        if isinstance(key, int):
            if key < 0:
                key = self.length + key

            validate_idx(index=key, length=self.length)
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

                validate_idx(index=idx, length=self.length)
                new_keys.append(idx)

            # get amount of times each index appears
            counter = Counter(new_keys)
            keys = counter.keys()
            items: list[tuple[int, Any]] = []

            for idx, val in enumerate(self.data):
                if idx in keys:
                    for i in range(counter.get(idx)):
                        items.append((idx, val))

                    if len(items) == len(new_keys):
                        break

            items_dict = dict(items)
            return [items_dict[x] for x in new_keys]

        raise TypeError(f'Index must be of type: int, list, or slice, not: {type(key)}')
