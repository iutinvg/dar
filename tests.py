from uuid import uuid4
import unittest
from db import Db, CHANGE_FRESH, CHANGE_DELETED, CHANGE_UPDATED
from db import Replacation


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

    def test_conflict(self):
        db = Db('s')
        value = str(uuid4())
        value2 = str(uuid4())
        item = db.post(value)

        # from pprint import pprint
        # pprint(db.data)

        try:
            db._conflict(item.uid, item.rev)
        except:
            self.fail('shouldnt raise here')

        db.put(item.uid, value2)

        with self.assertRaises(ValueError):
            db._conflict(item.uid, item.rev)

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

        item = db.get(item.uid, rev1)
        self.assertEqual(item.rev, rev1)
        self.assertEqual(item.value, value)

        item = db.get(item.uid, rev2)
        self.assertEqual(item.rev, rev2)
        self.assertEqual(item.value, value2)

    def test_rev_diff(self):
        db = Db('s')
        db2 = Db('t')
        value = str(uuid4())
        item = db.post(value)

        r = Replacation(db, db2)
        changes = db.get_changes()
        # for c in changes:
        #     print c
        prepared = r.prepare_changes(changes)
        # print prepared
        diff = db2.get_rev_diff(prepared)

        # print diff

        d = {
            item.uid: {
                'missing': [item.rev]
            }
        }
        self.assertEqual(diff, d)

    def test_rev_diff_2(self):
        db = Db('s')
        db2 = Db('t')
        value = str(uuid4())
        value2 = str(uuid4())

        item = db.post(value)
        item2 = db.put(item.uid, value2)

        item4 = db.post(uuid4())

        r = Replacation(db, db2)
        changes = db.get_changes()
        prepared = r.prepare_changes(changes)

        diff = db2.get_rev_diff(prepared)

        # print diff
        # return

        d = {
            item.uid: {
                'missing': [item.rev, item2.rev]
            },
            item4.uid: {
                'missing': [item4.rev]
            }
        }
        self.assertEqual(diff, d)


class ReplacationTest(unittest.TestCase):
    def test_prepare_changes(self):
        db = Db('s')
        db2 = Db('t')
        value = str(uuid4())
        value2 = str(uuid4())
        item = db.post(value)
        item2 = db.put(item.uid, value2)
        item3 = db.delete(item.uid)

        r = Replacation(db, db2)

        changes = db.get_changes()
        prepared = r.prepare_changes(changes)
        d = {item.uid: [item.rev, item2.rev, item3.rev]}
        self.assertEqual(prepared, d)

    def test_prepare_changes_2(self):
        db = Db('s')
        db2 = Db('t')
        value = str(uuid4())
        value2 = str(uuid4())
        item = db.post(value)
        item2 = db.put(item.uid, value2)
        item3 = db.delete(item.uid)

        item4 = db.post(uuid4())

        r = Replacation(db, db2)

        changes = db.get_changes()
        prepared = r.prepare_changes(changes)
        d = {
            item.uid: [item.rev, item2.rev, item3.rev],
            item4.uid: [item4.rev],
        }
        self.assertEqual(prepared, d)


if __name__ == '__main__':
    unittest.main()
