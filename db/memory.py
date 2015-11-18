from db.base import BaseDb


class MemoryDb(BaseDb):
    """docstring for MemoryDB"""
    def __init__(self, name):
        super(MemoryDb, self).__init__(name)
        self._data = {}
