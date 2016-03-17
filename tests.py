import hashlib
import unittest
from uuid import uuid4

from dar.db import DB, DataError, NotFoundError, Item


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

    def test_put_first_broken_rev(self):
        with self.assertRaises(DataError):
            self.db.put('val', 'uid', 'rev')

        with self.assertRaises(DataError):
            self.db.put('val', None, 'rev')

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

    def test_put_bulk(self):
        value = str(uuid4())
        first = self.db.put(value)
        rev = first.rev

        items = []

        for i in range(0, 100):
            value = str(uuid4())
            res = Item(
                value=value,
                rev=self.db.rev(value, rev),
                deleted=False
            )
            rev = res.rev
            items.append(res)

        self.db.put_bulk(first.uid, items)

        self.assertEqual(res.value, self.db.get(first.uid).value)

    def test_put_bulk_broken(self):
        value = str(uuid4())
        first = self.db.put(value)
        rev = first.rev

        items = []

        for i in range(0, 10):
            value = str(uuid4())
            res = Item(
                value=value,
                rev=self.db.rev(value, rev),
                deleted=False
            )
            rev = res.rev
            items.append(res)

        items[4] = Item('val', 'rev', False)
        with self.assertRaises(DataError):
            self.db.put_bulk(first.uid, items)

    def test_get_not_found(self):
        with self.assertRaises(NotFoundError):
            self.db.get('uid')

    def test_get(self):
        value = str(uuid4())
        res = self.db.put(value)
        self.assertEqual(res, self.db.get(res.uid))

    def test_remove(self):
        value = str(uuid4())
        res = self.db.put(value)
        self.db.remove(res.uid, res.rev)
        with self.assertRaises(NotFoundError):
            res = self.db.get(res.uid)

    def test_remove_and_put(self):
        value = str(uuid4())
        res = self.db.put(value)
        self.db.remove(res.uid, res.rev)
        with self.assertRaises(DataError):
            res = self.db.put('new vale', res.uid)

    def test_remove_bad_rev(self):
        value = str(uuid4())
        res = self.db.put(value)
        with self.assertRaises(DataError):
            self.db.remove(res.uid, 'bad_rev')

    def test_remove_not_found(self):
        with self.assertRaises(NotFoundError):
            self.db.remove('some-uid', 'bad_rev')

if __name__ == '__main__':
    unittest.main()
