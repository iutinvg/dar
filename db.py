from collections import defaultdict, namedtuple, OrderedDict
from itertools import islice
import hashlib
import uuid


class DataError(Exception):
    pass


class NotFoundError(Exception):
    pass


# InternalItems are stored in db
Item = namedtuple('Item', 'value, deleted')
HistoryItem = namedtuple('HistoryItem', 'uid, change_type')

# Result are return is operations result
Result = namedtuple('Result', 'uid, value, rev, deleted')
HistoryResult = namedtuple('HistoryResult', 'seq, change_type, rev')


class ChangeType:
    FRESH = 0
    UPDATED = 1
    DELETED = 2


class DB(object):
    def __init__(self, name):
        self.name = name
        self.storage = defaultdict(OrderedDict)
        self.changes = OrderedDict()
        self.local = {}  # local storage

    def put(self, value, uid=None, rev=None):
        if uid:
            change = ChangeType.UPDATED
        else:
            change = ChangeType.FRESH
            uid = self.uid()

        history = self.storage[uid]

        if len(history):
            if next(reversed(history)) != rev:
                raise DataError('rev not match')
        elif rev:
            raise DataError('rev does not make sense')

        rev = self.rev(value, rev)
        item = Item(value, False)
        history[rev] = item

        self.changes_put(uid, rev, change)

        return Result(uid, value, rev, False)

    def put_bulk(self, uid, items):
        if uid in self.storage:
            last_item = self.get(uid)
            rev = last_item.rev
        else:
            rev = None

        for i in items:
            if i.rev != self.rev(i.value, rev):
                raise DataError('bulk put broken integrity')
            rev = i.rev

        self.storage[uid].extend(items)

    def get(self, uid, rev=None):
        if uid not in self.storage:
            raise NotFoundError

        history = self.storage[uid]

        if rev:
            if rev not in history:
                raise NotFoundError('unknown revision')
        else:
            rev = next(reversed(history))

        item = history[rev]
        if item.deleted:
            raise NotFoundError('item deleted')

        return Result(uid, item.value, rev, False)

    def remove(self, uid, rev):
        if uid not in self.storage:
            raise NotFoundError

        history = self.storage[uid]

        if len(history):
            if next(reversed(history)) != rev:
                raise DataError('rev not match')
        else:
            raise DataError('rev does not make sense')

        rev = self.rev(None, rev)

        item = Item(None, True)
        history[rev] = item

        self.changes_put(uid, rev, ChangeType.DELETED)

        return Result(uid, item.value, rev, True)

    def changes_put(self, uid, rev, change_type):
        self.changes[rev] = HistoryItem(uid, change_type)

    def changes_get(self, since=0):
        for i, rev in enumerate(islice(self.changes.iterkeys(), since, None), since):
            yield HistoryResult(
                seq=i,
                change_type=self.changes[rev].change_type,
                rev=rev
            )

    def changes_get_grouped(self, since=0):
        res = defaultdict(list)
        for rev in islice(self.changes.iterkeys(), since, None):
            hi = self.changes[rev]
            res[hi.uid].append(rev)
        return res

    def changes_get_diff(self, grouped):
        res = defaultdict(list)
        for uid, revs in grouped.iteritems():
            for rev in revs:
                if rev not in self.changes:
                    res[uid].append(rev)
        return res

    def rev(self, value, rev):
        return hashlib.sha1(str(rev) + str(value)).hexdigest()

    def uid(self):
        return str(uuid.uuid4())
