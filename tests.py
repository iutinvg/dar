from uuid import uuid4
import unittest
from db import Db


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

        self.assertEqual(item['rev'][:2], '1-')
        self.assertEqual(item['value'], value)

    def test_put(self):
        db = Db()
        value = str(uuid4())
        value2 = str(uuid4())
        item = db.post(value)
        item = db.put(item['uid'], value2)

        self.assertEqual(item['rev'][:2], '2-')
        self.assertEqual(item['value'], value2)

    def test_get(self):
        db = Db()
        value = str(uuid4())
        value2 = str(uuid4())
        item = db.post(value)
        item = db.put(item['uid'], value2)

        item = db.get(item['uid'])
        self.assertEqual(item['rev'][:2], '2-')
        self.assertEqual(item['value'], value2)

    def test_delete(self):
        db = Db()
        value = str(uuid4())
        value2 = str(uuid4())
        item = db.post(value)
        item = db.put(item['uid'], value2)
        item = db.delete(item['uid'])

        with self.assertRaises(LookupError):
            item = db.get(item['uid'])


if __name__ == '__main__':
    unittest.main()
