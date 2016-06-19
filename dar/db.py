from collections import defaultdict, OrderedDict
from itertools import islice
import uuid

from .doc import Rev, Document
from .exceptions import DataError, NotFoundError


class DB(object):
    def __init__(self, name):
        self.name = name
        self.storage = defaultdict(Document)
        self.changes = OrderedDict()
        self.local = {}  # local storage

    def put(self, value, uid=None, rev=None):
        if uid is None:
            uid = self.uid()

        history = self.storage[uid]

        if len(history):
            if next(reversed(history)) != rev:
                raise DataError('rev not match')
        elif rev:
            raise DataError('rev does not make sense')

        # new_rev = self.rev(value, rev)
        new_rev = history.new_rev(value, rev)
        item = Rev(
            uid=uid,
            value=value,
            rev=new_rev,
            parent=rev,
        )
        history[new_rev] = item
        self.changes_put(uid, new_rev)

        return item

    def put_bulk(self, results):
        """Bulk adding of revisions.

        Similar to http://wiki.apache.org/couchdb/HTTP_Bulk_Document_API
        in `"new_edits":false`

        Params:
            `results` list of `Result` tuples
        """
        return [self._put_existing(i) for i in results]

    def _put_existing(self, i):
        if i.uid in self.storage:
            # ignore existing revisions
            document = self.storage[i.uid]
            if i.rev in document:
                return i
            rev = next(reversed(document))
        else:
            document = Document()
            rev = None

        if i.rev != document.new_rev(i.value, rev):
            return DataError('bulk put broken integrity %s' % str(i))

        self.storage[i.uid][i.rev] = i
        self.changes_put(i.uid, i.rev)
        return i

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

        return item

    def remove(self, uid, rev):
        if uid not in self.storage:
            raise NotFoundError

        history = self.storage[uid]

        if len(history):
            if next(reversed(history)) != rev:
                raise DataError('rev not match')
        else:
            raise DataError('rev does not make sense')

        new_rev = history.new_rev(None, rev)
        item = Rev(
            uid=uid,
            value=None,
            rev=new_rev,
            deleted=True,
            parent=rev,
        )

        history[rev] = item
        self.changes_put(uid, new_rev)

        return item

    def changes_put(self, uid, rev):
        self.changes[(uid, rev)] = None

    def changes_get(self, since=0):
        return islice(self.changes.iterkeys(), since, None)

    def changes_get_size(self):
        return len(self.changes)

    def changes_get_grouped(self, since=0):
        res = defaultdict(list)
        for uid, rev in self.changes_get(since):
            res[uid].append(rev)
        return res

    def changes_get_diff(self, grouped):
        res = defaultdict(list)
        for uid, revs in grouped.iteritems():
            for rev in revs:
                if (uid, rev) not in self.changes:
                    res[uid].append(rev)
        return res

    def local_put(self, uid, value):
        self.local[uid] = value

    def local_get(self, uid, default=0):
        return self.local.get(uid, default)

    def uid(self):
        return str(uuid.uuid4())
