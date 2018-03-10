from itertools import groupby
from operator import itemgetter


def unique_justseen(iterable, key=None):
    return map(next, map(itemgetter(1),
                         groupby(sorted(iterable, key=key), key)))
