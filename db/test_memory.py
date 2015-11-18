# from uuid import uuid4
import unittest
# from db import Db, CHANGE_FRESH, CHANGE_DELETED, CHANGE_UPDATED
# from db import Replacation, ConflictException


class MemoryDbTest(unittest.TestCase):
    def setUp(self):
        super(MemoryDbTest, self).setUp()
        print 'boo'

    def test_post(self):
        print 'foo'


if __name__ == '__main__':
    unittest.main()
