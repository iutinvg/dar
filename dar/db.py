from collections import defaultdict, OrderedDict, namedtuple
from itertools import islice
from functools import partial
import uuid

from .doc import Document, Revision
from .exceptions import DataError, NotFoundError

Result = namedtuple('Result', 'uid rev value deleted parent')
Res = partial(Result, deleted=False)


class DB(object):
    def __init__(self, name):
        self.name = name
        self.storage = defaultdict(Document)
        self.changes = OrderedDict()
        self.local = {}  # local storage

    def put(self, value, uid=None, rev=None):
        if uid is None:
            uid = self.uid()

        document = self.storage[uid]

        if not len(document) and rev is not None:
            raise DataError('passing rev for new doc')

        new_rev, _ = document.put(value, rev)

        self.changes_put(uid, new_rev)

        return Result(uid, new_rev, value, False, rev)

    def put_bulk(self, results):
        """Bulk adding of revisions.

        Similar to http://wiki.apache.org/couchdb/HTTP_Bulk_Document_API
        in `"new_edits":false`

        Params:
            `results` list of `Result` tuples
        """
        return [self._put_existing(result) for result in results]

    def _put_existing(self, result):
        rev = result.rev
        revision = Revision(
            value=result.value,
            deleted=result.deleted,
            parent=result.parent
        )

        document = self.storage[result.uid]
        try:
            document.put_existing(rev, revision)
        except DataError as e:
            return e
        self.changes_put(result.uid, rev)
        return result

    def get(self, uid, rev=None):
        if uid not in self.storage:
            raise NotFoundError

        document = self.storage[uid]
        rev, revision = document.get(rev)

        if revision.deleted:
            raise NotFoundError

        return Result(uid, rev, revision.value, revision.deleted, revision.parent)

    def remove(self, uid, rev):
        if uid not in self.storage:
            raise NotFoundError

        document = self.storage[uid]

        new_rev, revision = document.remove(rev)

        self.changes_put(uid, new_rev)

        return Result(uid, new_rev, revision.value, revision.deleted, revision.parent)

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
