from collections import namedtuple, OrderedDict
from functools import partial


class ChangeType:
    FRESH = 0
    UPDATED = 1
    DELETED = 2


# Result is operations result
Revision = namedtuple('Revision', 'uid value rev deleted parent seq change_type meta conflict')
Rev = partial(Revision, meta=None, deleted=False, seq=0, change_type=ChangeType.UPDATED, conflict=None)


class Document(OrderedDict):
    pass
