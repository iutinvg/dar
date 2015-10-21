from uuid import uuid4
import unittest
from db import Db, CHANGE_FRESH, CHANGE_DELETED, CHANGE_UPDATED


class DbTest(unittest.TestCase):

    def test_rev(self):
        db = Db()
        self.assertEqual(db._get_rev()[:2], '1-')
        self.assertEqual(db._get_rev('1-')[:2], '2-')
        self.assertEqual(db._get_rev('boo')[:2], '1-')

    def test_post(self):
        db = Db()
        value = str(uuid4())
        item = db.post(value)

        self.assertEqual(item.rev[:2], '1-')
        self.assertEqual(item.value, value)

        changes = db.get_changes()
        self.assertEqual(len(changes), 1)
        self.assertEqual(changes[0].change, CHANGE_FRESH)
        self.assertEqual(changes[0].rev, item.rev)

    def test_put(self):
        db = Db()
        value = str(uuid4())
        value2 = str(uuid4())
        item = db.post(value)
        item2 = db.put(item.uid, value2)

        self.assertEqual(item2.rev[:2], '2-')
        self.assertEqual(item2.value, value2)

        changes = db.get_changes()
        self.assertEqual(len(changes), 2)
        self.assertEqual(changes[0].change, CHANGE_FRESH)
        self.assertEqual(changes[0].rev, item.rev)
        self.assertEqual(changes[1].change, CHANGE_UPDATED)
        self.assertEqual(changes[1].rev, item2.rev)

    def test_delete(self):
        db = Db()
        value = str(uuid4())
        value2 = str(uuid4())
        item = db.post(value)
        item2 = db.put(item.uid, value2)
        item3 = db.delete(item.uid)

        self.assertTrue(item3.deleted)
        self.assertFalse(item2.deleted)
        self.assertFalse(item.deleted)

        changes = db.get_changes()
        self.assertEqual(len(changes), 3)
        self.assertEqual(changes[0].change, CHANGE_FRESH)
        self.assertEqual(changes[0].rev, item.rev)
        self.assertEqual(changes[1].change, CHANGE_UPDATED)
        self.assertEqual(changes[1].rev, item2.rev)
        self.assertEqual(changes[2].change, CHANGE_DELETED)
        self.assertEqual(changes[2].rev, item3.rev)

        with self.assertRaises(LookupError):
            item = db.get(item.uid)

    def test_get(self):
        db = Db()
        value = str(uuid4())
        value2 = str(uuid4())
        item = db.post(value)
        item = db.put(item.uid, value2)

        item = db.get(item.uid)
        self.assertEqual(item.rev[:2], '2-')
        self.assertEqual(item.value, value2)

    def test_get_by_rev(self):
        db = Db()
        value = str(uuid4())
        value2 = str(uuid4())
        item = db.post(value)
        rev1 = item.rev
        item = db.put(item.uid, value2)
        rev2 = item.rev

        item = db.get_by_rev(rev1)
        self.assertEqual(item.rev, rev1)
        self.assertEqual(item.value, value)

        item = db.get_by_rev(rev2)
        self.assertEqual(item.rev, rev2)
        self.assertEqual(item.value, value2)


if __name__ == '__main__':
    unittest.main()
