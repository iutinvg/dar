import hashlib
import uuid

from document import Document
from change import Change, CHANGE_FRESH, CHANGE_UPDATED


class BaseDriver(object):
    def __init__(self, name):
        self.name = name

    def post(self, value, uid=None):
        """
        Create a new document in a database.

        value -- value to store
        returns ``Document`` instance
        """
        raise NotImplementedError

    def put(self, uid, value):
        raise NotImplementedError

    def get(self, uid, rev=None):
        raise NotImplementedError

    def get_changes(self, since):
        """Return list of changes, see ``Change`` class"""
        raise NotImplementedError

    def get_local(self, uid):
        """
        Get a value from non-replicated storage

        returns the stored value (not a ``Document`` instance)
        """
        raise NotImplementedError

    def set_local(self, uid, value):
        """Set a value to non-replicated storage"""
        raise NotImplementedError


class MemoryDriver(BaseDriver):
    """Driver to store data in memory"""
    def __init__(self, name):
        super(MemoryDriver, self).__init__(name)
        self.data = {}
        self.changes = []

    def post(self, value, uid=None):
        if not uid:
            uid = hashlib.sha1(str(uuid.uuid4())).hexdigest()
        return self.put(uid, value)

    def put(self, uid, value):
        try:
            document = self.data[uid]
            document.update(value)
            self._add_change(CHANGE_UPDATED, uid, document.revision(), False)
        except KeyError:
            document = Document(value)
            self.data[uid] = document
            self._add_change(CHANGE_FRESH, uid, document.revision(), False)
        return document

    def get(self, uid, rev=None):
        return self.data[uid].value(rev)

    def get_changes(self, since):
        return self.changes[since:]

    def _add_change(self, change, uid, rev, deleted=False):
        c = Change(change, uid, rev, deleted)
        self.changes.append(c)

    def get_local(self, uid):
        return self.local.get(uid)

    def set_local(self, uid, value):
        self.local[uid] = value
