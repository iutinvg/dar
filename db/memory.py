import hashlib
import uuid

from db.base import BaseDb
from db.document import Document


class MemoryDb(BaseDb):
    """docstring for MemoryDB"""
    def __init__(self, name):
        super(MemoryDb, self).__init__(name)
        self.data = {}

    def post(self, value):
        uid = hashlib.sha1(str(uuid.uuid4())).hexdigest()
        document = Document(value)
        self.data[uid] = document
        return document

    def put(self, uid, value):
        document = self.data[uid]
        document.update(value)
        return document

    def get(self, uid, rev=None):
        return self.data[uid].value(rev)
