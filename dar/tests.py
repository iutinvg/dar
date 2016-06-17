from collections import defaultdict
import hashlib
import unittest
from uuid import uuid4
import random

from .db import DB
from .exceptions import DataError, NotFoundError
from .doc import Rev, new_rev, Document, Revision
from .repl import Repl


class DocTest(unittest.TestCase):
    def test_init(self):
        doc = Document()
        self.assertIsNone(doc.winner)
        self.assertEqual(doc.conflicts, set())

    def test_render(self):
        doc = Document()

        revision1 = Revision('0', False, None)
        rev1 = '1-aaa'
        doc[rev1] = revision1
        doc.winner = rev1

        revision2 = Revision('1', False, rev1)
        rev2 = '2-aaa'
        doc[rev2] = revision2
        doc.winner = rev2

        revision3 = Revision('-1', False, rev1)
        rev3 = '2-bbb'
        doc[rev3] = revision3
        doc.winner = rev3
        doc.conflicts = set([rev2])

        revision4 = Revision('2', False, rev1)
        rev4 = '2-ccc'
        doc[rev4] = revision4

        revision5 = Revision('4', False, rev4)
        rev5 = '3-ccc'
        doc[rev5] = revision5

        self.assertEqual('1-aaa\n +-- 2-aaa\n +-- 2-bbb\n +-- 2-ccc\n     +-- 3-ccc', doc.__str__())

    def test_update_winner_first(self):
        doc = Document()

        revision1 = Revision('1', False, None)
        rev1 = '1-aaa'
        doc[rev1] = revision1

        doc.update_winner(rev1, revision1)
        self.assertEqual(doc.winner, rev1)
        self.assertEqual(doc.conflicts, set())

    def test_update_winner_second(self):
        doc = Document()

        revision1 = Revision('1', False, None)
        rev1 = '1-aaa'
        doc[rev1] = revision1
        doc.winner = rev1

        revision2 = Revision('2', False, rev1)
        rev2 = '2-aaa'
        doc[rev2] = revision2

        doc.update_winner(rev2, revision2)
        self.assertEqual(doc.winner, rev2)
        self.assertEqual(doc.conflicts, set())

    def test_update_winner_conflict_ascii(self):
        doc = Document()

        revision1 = Revision('0', False, None)
        rev1 = '1-aaa'
        doc[rev1] = revision1
        doc.winner = rev1

        revision2 = Revision('1', False, rev1)
        rev2 = '2-aaa'
        doc[rev2] = revision2
        doc.winner = rev2

        revision3 = Revision('-1', False, rev1)
        rev3 = '2-bbb'
        doc[rev3] = revision3

        doc.update_winner(rev3, revision3)
        self.assertEqual(doc.winner, rev3)
        self.assertEqual(doc.conflicts, set([rev2]))

    def test_update_winner_conflict_length(self):
        doc = Document()

        revision1 = Revision('0', False, None)
        rev1 = '1-aaa'
        doc[rev1] = revision1
        doc.winner = rev1

        revision2 = Revision('1', False, rev1)
        rev2 = '2-aaa'
        doc[rev2] = revision2
        doc.winner = rev2

        revision3 = Revision('-1', False, rev1)
        rev3 = '2-bbb'
        doc[rev3] = revision3
        doc.winner = rev3
        doc.conflicts = set([rev2])

        revision4 = Revision('100', False, rev2)
        rev4 = '3-aaa'
        doc[rev4] = revision4

        doc.update_winner(rev4, revision4)
        self.assertEqual(doc.winner, rev4)
        self.assertEqual(doc.conflicts, set([rev3]))

    def test_update_winner_multiple_conflicts(self):
        doc = Document()

        revision1 = Revision('0', False, None)
        rev1 = '1-aaa'
        doc[rev1] = revision1
        doc.winner = rev1

        revision2 = Revision('1', False, rev1)
        rev2 = '2-aaa'
        doc[rev2] = revision2
        doc.winner = rev2

        revision3 = Revision('-1', False, rev1)
        rev3 = '2-bbb'
        doc[rev3] = revision3
        doc.winner = rev3
        doc.conflicts = set([rev2])

        revision4 = Revision('2', False, rev1)
        rev4 = '2-ccc'
        doc[rev4] = revision4

        doc.update_winner(rev4, revision4)
        self.assertEqual(doc.winner, rev4)
        self.assertEqual(doc.conflicts, set([rev2, rev3]))


class DBTest(unittest.TestCase):
    def setUp(self):
        super(DBTest, self).setUp()

        self.db = DB(str(uuid4()))

    def test_put_first(self):
        value = str(uuid4())
        res = self.db.put(value)

        self.assertEqual(len(self.db.storage), 1)
        self.assertEqual(res.value, value)
        self.assertEqual(res.rev, '1-' + hashlib.sha1('None' + str(value)).hexdigest())
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
            self.assertTrue(res.rev.startswith(str(i + 1) + '-'))
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
            self.assertEqual(res.rev, '1-' + hashlib.sha1('None' + str(value)).hexdigest())
            self.assertIsNotNone(res.uid)

    def test_put_bulk(self):
        value = str(uuid4())
        first = self.db.put(value)
        rev = first.rev

        items = []

        for i in range(100):
            value = str(uuid4())
            res = Rev(
                uid=first.uid,
                value=value,
                rev=new_rev(value, rev, lambda: str(i + 2)),
                parent=rev,
            )
            rev = res.rev
            items.append(res)

        self.db.put_bulk(items)

        self.assertEqual(res.value, self.db.get(first.uid).value)

    def test_put_bulk_new(self):
        rev = None
        uid = str(uuid4())
        items = []

        for i in range(100):
            value = str(uuid4())
            res = Rev(
                uid=uid,
                value=value,
                rev=new_rev(value, rev, lambda: str(i + 1)),
                parent=rev,
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
            res = Rev(
                uid=uid,
                value=value,
                rev=new_rev(value, rev),
                parent=rev,
            )
            rev = res.rev
            items.append(res)

        res = Rev(
            uid=uid,
            value=None,
            rev=new_rev(None, rev),
            deleted=True,
            parent=rev,
        )
        items.append(res)

        self.db.put_bulk(items)

        with self.assertRaises(NotFoundError):
            self.db.get(uid)

    def test_put_bulk_intersection_same(self):
        """Test the case when existing revs are passed to put_bulk.

        Just the same docs are given.
        """
        items = []
        rev = None
        uid = 'same'

        for i in range(5):
            doc = self.db.put(i, uid, rev)
            rev = doc.rev
            items.append(doc)

        statuses = self.db.put_bulk(items)
        for i, e in enumerate(statuses):
            self.assertEqual(items[i], e)

    def test_put_bulk_intersection(self):
        """Test the case when existing revs are passed to put_bulk.

        Just the same docs are given.
        """
        items = []
        rev = None
        uid = 'same'

        for i in range(5):
            doc = self.db.put(i, uid, rev)
            rev = doc.rev
            items.append(doc)

        doc = Rev(
            uid=uid,
            value=10,
            rev=new_rev(10, rev, lambda: str(len(items) + 1)),
            parent=rev
        )
        items.append(doc)

        statuses = self.db.put_bulk(items)
        for i, e in enumerate(statuses):
            self.assertEqual(items[i], e)

    def test_put_bulk_broken(self):
        value = str(uuid4())
        first = self.db.put(value)
        rev = first.rev

        items = []

        for i in range(0, 10):
            value = str(uuid4())
            res = Rev(
                uid=first.uid,
                value=value,
                rev=new_rev(value, rev),
                parent=rev,
            )
            rev = res.rev
            items.append(res)

        items[4] = Rev(
            uid=items[4].uid,
            value='val',
            rev='wrong-rev',
            deleted=items[4].deleted,
            parent=items[4].rev,
        )
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
        print changes

        self.assertEqual(changes[0].rev, res1.rev)

        self.assertEqual(changes[1].rev, res2.rev)

        changes = list(self.db.changes_get(1))

        self.assertEqual(changes[0].rev, res2.rev)

    def test_changes_update(self):
        res1 = self.db.put('val')
        res2 = self.db.put('value', res1.uid, res1.rev)

        changes = list(self.db.changes_get())

        self.assertEqual(changes[0].rev, res1.rev)
        self.assertEqual(changes[1].rev, res2.rev)

        changes = list(self.db.changes_get(1))

        self.assertEqual(changes[0].rev, res2.rev)

    def test_changes_remove(self):
        res1 = self.db.put('val')
        res2 = self.db.remove(res1.uid, res1.rev)

        changes = list(self.db.changes_get())

        self.assertEqual(changes[0].rev, res1.rev)
        self.assertEqual(changes[1].rev, res2.rev)

        changes = list(self.db.changes_get(1))

        self.assertEqual(changes[0].rev, res2.rev)

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

    def test_replicate_empty(self):
        self.repl.replicate()
        self.assertEqual(self.source.storage, self.target.storage)

    def test_replicate_single(self):
        s_res1 = self.source.put('val1')

        self.repl.replicate()
        self.assertEqual(self.source.storage, self.target.storage)
        self.assertEqual(self.target.get(s_res1.uid), s_res1)

    def test_replicate_single_double(self):
        s_res1 = self.source.put('val1')

        self.repl.replicate()
        self.repl.replicate()

        self.assertEqual(self.source.storage, self.target.storage)
        self.assertEqual(self.target.get(s_res1.uid), s_res1)

    def test_replicate_several(self):
        res = self.source.put(str(uuid4()))
        rev = res.rev
        uid = res.uid

        for i in range(0, 10):
            value = str(uuid4())
            res = self.source.put(value, uid, rev)
            rev = res.rev

        self.repl.replicate()

        self.assertEqual(self.source.storage, self.target.storage)

    def test_replicate_sequence(self):
        self._change_db(self.source, 1000)
        self.repl.replicate()
        # self.assertEqual(self.source.storage, self.target.storage)
        self._assert_db_equal(self.source, self.target)

        self._change_db(self.source, 1000)
        self.repl.replicate()
        # self.assertEqual(self.source.storage, self.target.storage)
        self._assert_db_equal(self.source, self.target)

        self._change_db(self.source, 1000)
        self.repl.replicate()
        # self.assertEqual(self.source.storage, self.target.storage)
        self._assert_db_equal(self.source, self.target)

        self.assertEqual(len(self.source.storage), 3000)

    def test_replicate_bidirect(self):
        rrepl = Repl(self.target, self.source)
        n = 100
        c = 50
        for i in range(10):
            self._change_db(self.source, n, c)
            self._change_db(self.target, n, 0)

            self.repl.replicate()
            rrepl.replicate()

            self._assert_db_equal(self.source, self.target)

    def _assert_db_equal(self, db1, db2):
        self.assertEqual(len(db1.storage), len(db2.storage))
        for k in db1.storage.iterkeys():
            self.assertEqual(db1.get(k), db2.get(k))

    def _change_db(self, db, new_count, change_percents=50):
        for _ in range(new_count):
            db.put(str(uuid4()))

        number = int(len(db.storage) / 100.0 * change_percents)
        for key in random.sample(db.storage.keys(), number):
            res = db.get(key)
            db.put(str(uuid4()), res.uid, res.rev)


if __name__ == '__main__':
    unittest.main()
