import hashlib
import unittest
from uuid import uuid4

from dar.db import DB, DataError


# from uuid import uuid4
# import unittest
# # from db import Db, CHANGE_FRESH, CHANGE_DELETED, CHANGE_UPDATED
# # from db import Replacation, ConflictException

# from document import Document

class DBTest(unittest.TestCase):
    def setUp(self):
        super(DBTest, self).setUp()

        self.db = DB(str(uuid4()))

    def test_put_first(self):
        value = str(uuid4())
        res = self.db.put(value)

        self.assertEqual(len(self.db.storage), 1)
        self.assertEqual(res.value, value)
        self.assertEqual(res.rev, hashlib.sha1('None' + str(value)).hexdigest())
        self.assertIsNotNone(res.uid)

    def test_put_broken_rev(self):
        res = self.db.put(str(uuid4()))
        with self.assertRaises(DataError):
            self.db.put(str(uuid4()), res.uid, 'wrong-rev')

    def test_put_the_same(self):
        uid = None
        rev = None
        for i in range(0, 100):
            value = str(uuid4())
            res = self.db.put(value, uid, rev)

            self.assertEqual(len(self.db.storage), 1)
            self.assertEqual(len(self.db.storage[res.uid]), i + 1)
            self.assertEqual(res.value, value)
            self.assertEqual(res.rev, hashlib.sha1(str(rev) + str(value)).hexdigest())
            self.assertIsNotNone(res.uid)

            rev = res.rev
            uid = res.uid

    def test_put_different(self):
        for i in range(0, 100):
            uid = str(uuid4())
            value = str(uuid4())
            res = self.db.put(value, uid)

            self.assertEqual(len(self.db.storage), i + 1)
            self.assertEqual(len(self.db.storage[res.uid]), 1)
            self.assertEqual(res.value, value)
            self.assertEqual(res.rev, hashlib.sha1('None' + str(value)).hexdigest())
            self.assertIsNotNone(res.uid)

if __name__ == '__main__':
    unittest.main()
