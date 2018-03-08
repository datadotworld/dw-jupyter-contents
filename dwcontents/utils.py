from itertools import imap, groupby
from operator import itemgetter


def unique_justseen(iterable, key=None):
    return imap(next, imap(itemgetter(1),
                           groupby(sorted(iterable, key=key), key)))