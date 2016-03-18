from collections import defaultdict, namedtuple
import hashlib
import uuid


class DataError(Exception):
    pass


class NotFoundError(Exception):
    pass


# InternalItems are stored in db
Item = namedtuple('Item', 'value, rev, deleted')

# Result are return is operations result
Result = namedtuple('Result', 'uid, value, rev')


class ChangeType:
    FRESH = 0
    UPDATED = 1
    DELETED = 2


class DB(object):
    def __init__(self, name):
        self.name = name
        self.storage = defaultdict(list)
        self.changes = []
        self.local = {}  # local storage

    def put(self, value, uid=None, rev=None):
        if uid:
            change = ChangeType.UPDATED
        else:
            change = ChangeType.FRESH
            uid = self.uid()

        history = self.storage[uid]

        if len(history):
            if history[-1].rev != rev:
                raise DataError('rev not match')
        elif rev:
            raise DataError('rev does not make sense')

        item = Item(value, self.rev(value, rev), False)
        history.append(item)

        self.changes_put(Result(uid, change, item.rev))

        return Result(uid, value, item.rev)

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

    def get(self, uid):
        if uid not in self.storage:
            raise NotFoundError

        item = self.storage[uid][-1]
        if item.deleted:
            raise NotFoundError('item deleted')

        return Result(uid, item.value, item.rev)

    def remove(self, uid, rev):
        if uid not in self.storage:
            raise NotFoundError

        history = self.storage[uid]

        if len(history):
            if history[-1].rev != rev:
                raise DataError('rev not match')
        else:
            raise DataError('rev does not make sense')

        item = Item(None, self.rev(None, rev), True)
        history.append(item)

        self.changes_put(Result(uid, ChangeType.DELETED, item.rev))

        return Result(uid, item.value, item.rev)

    def changes_put(self, value):
        self.changes.append(value)

    def changes_get(self, since=0):
        return self.changes[since:]

    def rev(self, value, rev):
        return hashlib.sha1(str(rev) + str(value)).hexdigest()

    def uid(self):
        return str(uuid.uuid4())
