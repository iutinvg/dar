from collections import namedtuple, OrderedDict
from functools import partial
import hashlib


class ChangeType:
    FRESH = 0
    UPDATED = 1
    DELETED = 2


# Result is operations result
Revision = namedtuple('Revision', 'uid value rev deleted parent seq change_type meta conflict')
Rev = partial(Revision, meta=None, deleted=False, seq=0, change_type=ChangeType.UPDATED, conflict=None)


class Document(OrderedDict):
    def new_rev(self, value, rev):
        return new_rev(value, rev)


def new_rev(value, rev, prefix=None):
    p = prefix() + '-' if prefix else ''
    return p + hashlib.sha1(str(rev) + str(value)).hexdigest()
