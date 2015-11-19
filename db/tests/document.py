from uuid import uuid4
import unittest
# from db import Db, CHANGE_FRESH, CHANGE_DELETED, CHANGE_UPDATED
# from db import Replacation, ConflictException

from db.document import Document


class DocumentTest(unittest.TestCase):
    def setUp(self):
        super(DocumentTest, self).setUp()

    def test_value(self):
        doc = Document(str(uuid4()))
        self.assertEquals(doc.value(), doc.revs.values()[-1])

        doc.update(str(uuid4()))
        self.assertEquals(doc.value(), doc.revs.values()[-1])

    def test_value_by_rev(self):
        doc = Document(str(uuid4()))
        r1 = doc.revision()
        self.assertEquals(doc.value(r1), doc.revs[r1])

        doc.update(str(uuid4()))
        r2 = doc.revision()

        self.assertEquals(doc.value(r1), doc.revs[r1])
        self.assertEquals(doc.value(r2), doc.revs[r2])

    def test_update(self):
        doc = Document(str(uuid4()))
        self.assertEquals(len(doc.revs.keys()), 1)
        doc.update(str(uuid4()))
        self.assertEquals(len(doc.revs.keys()), 2)

        print doc.revs


if __name__ == '__main__':
    unittest.main()
