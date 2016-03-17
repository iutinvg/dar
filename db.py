from collections import defaultdict, namedtuple
import hashlib
import uuid


class DataError(Exception):
    pass


class NotFoundError(Exception):
    pass


# InternalItems are stored in db
Item = namedtuple('Item', 'value, rev')

# Result are return is operations result
Result = namedtuple('Result', 'uid, value, rev')


class DB(object):
    def __init__(self, name):
        self.name = name
        self.storage = defaultdict(list)

    def put(self, value, uid=None, rev=None):
        if not uid:
            uid = self.uid()

        history = self.storage[uid]

        if len(history) and history[-1].rev != rev:
            raise DataError('rev not match')

        item = Item(value, self.rev(value, rev))
        history.append(item)
        return Result(uid, value, item.rev)

    def rev(self, value, rev):
        return hashlib.sha1(str(rev) + str(value)).hexdigest()

    def uid(self):
        return str(uuid.uuid4())
