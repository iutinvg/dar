import hashlib
import uuid

from db.base import BaseDriver
from db.document import Document


class MemoryDriver(BaseDriver):
    """Driver to keep values in memory"""
    def __init__(self, name):
        super(MemoryDriver, self).__init__(name)
        self.data = {}

    def post(self, value, uid=None):
        if not uid:
            uid = hashlib.sha1(str(uuid.uuid4())).hexdigest()
        return self.put(uid, value)

    def put(self, uid, value):
        try:
            document = self.data[uid]
            document.update(value)
        except KeyError:
            document = Document(value)
            self.data[uid] = document
        return document

    def get(self, uid, rev=None):
        return self.data[uid].value(rev)
