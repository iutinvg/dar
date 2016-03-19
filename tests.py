from collections import defaultdict
import hashlib
import unittest
from uuid import uuid4
import random

from dar.db import DB, DataError, NotFoundError, ChangeType
from dar.db import Result
from dar.repl import Repl


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
            res = Result(
                uid=first.uid,
                value=value,
                rev=self.db.rev(value, rev),
                deleted=False
            )
            rev = res.rev
            items.append(res)

        self.db.put_bulk(items)

        self.assertEqual(res.value, self.db.get(first.uid).value)

    def test_put_bulk_new(self):
        rev = None
        uid = str(uuid4())
        items = []

        for i in range(0, 100):
            value = str(uuid4())
            res = Result(
                uid=uid,
                value=value,
                rev=self.db.rev(value, rev),
                deleted=False
            )
            rev = res.rev
            items.append(res)

        self.db.put_bulk(items)

        self.assertEqual(res.value, self.db.get(uid).value)

    def test_put_bulk_deleted(self):
        rev = None
        uid = str(uuid4())
        items = []

        for i in range(0, 100):
            value = str(uuid4())
            res = Result(
                uid=uid,
                value=value,
                rev=self.db.rev(value, rev),
                deleted=False
            )
            rev = res.rev
            items.append(res)

        res = Result(
            uid=uid,
            value=None,
            rev=self.db.rev(None, rev),
            deleted=True
        )
        items.append(res)

        self.db.put_bulk(items)

        with self.assertRaises(NotFoundError):
            self.db.get(uid)

    def test_put_bulk_broken(self):
        value = str(uuid4())
        first = self.db.put(value)
        rev = first.rev

        items = []

        for i in range(0, 10):
            value = str(uuid4())
            res = Result(
                uid=first.uid,
                value=value,
                rev=self.db.rev(value, rev),
                deleted=False
            )
            rev = res.rev
            items.append(res)

        items[4] = Result(first.uid, 'val', 'wrong-rev', False)
        statuses = self.db.put_bulk(items)

        self.assertIn('broken', str(statuses[4]))
        self.assertIn('wrong-rev', str(statuses[4]))

    def test_get_not_found(self):
        with self.assertRaises(NotFoundError):
            self.db.get('uid')

    def test_get(self):
        value = str(uuid4())
        res = self.db.put(value)
        self.assertEqual(res, self.db.get(res.uid))

    def test_get_with_rev(self):
        value1 = str(uuid4())
        res1 = self.db.put(value1)
        first_rev = res1.rev
        value2 = str(uuid4())
        self.db.put(value2, res1.uid, res1.rev)

        self.assertEqual(res1, self.db.get(res1.uid, first_rev))
        self.assertEqual(res1.value, value1)

    def test_get_with_rev_wrong(self):
        value1 = str(uuid4())
        res1 = self.db.put(value1)

        with self.assertRaises(NotFoundError):
            self.db.get(res1.uid, 'wrong_rev')

    def test_remove(self):
        value = str(uuid4())
        res = self.db.put(value)
        res = self.db.remove(res.uid, res.rev)
        self.assertEqual(res.deleted, True)
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

    def test_changes_new(self):
        res1 = self.db.put('val1')
        res2 = self.db.put('val2')

        changes = list(self.db.changes_get())

        self.assertEqual(changes[0].change_type, ChangeType.FRESH)
        self.assertEqual(changes[0].rev, res1.rev)
        self.assertEqual(changes[0].seq, 0)

        self.assertEqual(changes[1].change_type, ChangeType.FRESH)
        self.assertEqual(changes[1].rev, res2.rev)
        self.assertEqual(changes[1].seq, 1)

        changes = list(self.db.changes_get(1))

        self.assertEqual(changes[0].change_type, ChangeType.FRESH)
        self.assertEqual(changes[0].rev, res2.rev)
        self.assertEqual(changes[0].seq, 1)

    def test_changes_update(self):
        res1 = self.db.put('val')
        res2 = self.db.put('value', res1.uid, res1.rev)

        changes = list(self.db.changes_get())

        self.assertEqual(changes[0].change_type, ChangeType.FRESH)
        self.assertEqual(changes[0].rev, res1.rev)
        self.assertEqual(changes[0].seq, 0)
        self.assertEqual(changes[1].change_type, ChangeType.UPDATED)
        self.assertEqual(changes[1].rev, res2.rev)
        self.assertEqual(changes[1].seq, 1)

        changes = list(self.db.changes_get(1))

        self.assertEqual(changes[0].change_type, ChangeType.UPDATED)
        self.assertEqual(changes[0].rev, res2.rev)
        self.assertEqual(changes[0].seq, 1)

    def test_changes_remove(self):
        res1 = self.db.put('val')
        res2 = self.db.remove(res1.uid, res1.rev)

        changes = list(self.db.changes_get())

        self.assertEqual(changes[0].change_type, ChangeType.FRESH)
        self.assertEqual(changes[0].rev, res1.rev)
        self.assertEqual(changes[0].seq, 0)
        self.assertEqual(changes[1].change_type, ChangeType.DELETED)
        self.assertEqual(changes[1].rev, res2.rev)
        self.assertEqual(changes[1].seq, 1)

        changes = list(self.db.changes_get(1))

        self.assertEqual(changes[0].change_type, ChangeType.DELETED)
        self.assertEqual(changes[0].rev, res2.rev)
        self.assertEqual(changes[0].seq, 1)

    def test_changes_grouped(self):
        res1 = self.db.put('val1')
        res2 = self.db.put('val2')
        res3 = self.db.put('val3', res1.uid, res1.rev)

        grouped = self.db.changes_get_grouped()

        self.assertEqual(len(grouped), 2)
        self.assertIn(res1.uid, grouped)
        self.assertIn(res2.uid, grouped)

        self.assertIn(res1.rev, grouped[res1.uid])
        self.assertIn(res3.rev, grouped[res1.uid])

        self.assertIn(res2.rev, grouped[res2.uid])

    def test_changed_get_diff_same(self):
        res1 = self.db.put('val1')
        self.db.put('val2')
        self.db.put('val3', res1.uid, res1.rev)

        grouped = self.db.changes_get_grouped()

        self.assertEqual(defaultdict(list), self.db.changes_get_diff(grouped))

    def test_changed_get_diff_not_empty(self):
        res1 = self.db.put('val1')
        self.db.put('val2')
        self.db.put('val3', res1.uid, res1.rev)

        grouped = self.db.changes_get_grouped()
        grouped[res1.uid].append('new-rev')

        expected = defaultdict(list)
        expected[res1.uid].append('new-rev')

        self.assertEqual(expected, self.db.changes_get_diff(grouped))

    def test_seq(self):
        self.assertEqual(self.db.changes_get_size(), 0)

        res1 = self.db.put('val1')
        self.db.put('val2')
        self.db.put('val3', res1.uid, res1.rev)

        self.assertEqual(self.db.changes_get_size(), 3)

    def test_local_put(self):
        r = random.randint(1, 1000)
        uid = str(uuid4())
        self.db.local_put(uid, r)

        self.assertEqual(self.db.local[uid], r)

    def test_local_get(self):
        uid = str(uuid4())
        self.assertEqual(self.db.local_get(uid), 0)

        r = random.randint(1, 1000)
        self.db.local[uid] = r
        self.assertEqual(self.db.local_get(uid), r)


class ReplTest(unittest.TestCase):
    def setUp(self):
        super(ReplTest, self).setUp()
        self.source = DB(str(uuid4()))
        self.target = DB(str(uuid4()))
        self.repl = Repl(self.source, self.target)

    def test_uid(self):
        source = DB(str(uuid4()))
        target = DB(str(uuid4()))
        repl = Repl(source, target)
        self.assertEqual(
            hashlib.sha1(source.name + target.name).hexdigest(),
            repl.uid
        )

    def test_get_last_seq(self):
        self.assertEqual(self.repl.get_last_seq(), 0)

        r = random.randint(1, 1000)
        self.target.local[self.repl.uid] = r
        self.assertEqual(self.repl.get_last_seq(), r)

    def test_get_diff_docs_single(self):
        s_res1 = self.source.put('val1')

        d = {s_res1.uid: [s_res1.rev]}

        diff_docs = list(self.repl.get_diff_docs(d))

        self.assertEqual([s_res1], diff_docs)

    def test_get_diff_docs_two(self):
        s_res1 = self.source.put('val1')
        s_res2 = self.source.put('val2')

        d = {
            s_res1.uid: [s_res1.rev],
            s_res2.uid: [s_res2.rev]
        }

        diff_docs = list(self.repl.get_diff_docs(d))

        self.assertIn(s_res1, diff_docs)
        self.assertIn(s_res2, diff_docs)
        self.assertEqual(2, len(diff_docs))

    def test_get_diff_docs_empty(self):
        diff_docs = list(self.repl.get_diff_docs({}))
        self.assertEqual(0, len(diff_docs))

if __name__ == '__main__':
    unittest.main()
