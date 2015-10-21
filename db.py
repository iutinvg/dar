from uuid import uuid4
import re
from collections import OrderedDict


CHANGE_FRESH = 'fresh'
CHANGE_UPDATED = 'updated'
CHANGE_DELETED = 'deleted'

K_VALUE = 'value'
K_REV = 'rev'
K_UID = 'uid'


class Db(object):
    def __init__(self):
        self.data = {}
        self.changes = []

    def get(self, uid):
        item = self._get_item(uid)
        if CHANGE_DELETED in item[K_VALUE]:
            raise LookupError
        return self._get_item(uid)

    def put(self, uid, value):
        od = self.data[uid]
        rev = next(reversed(od))
        od[self._get_rev(rev)] = value
        return self._get_item(uid)

    def post(self, value):
        uid = str(uuid4())
        od = OrderedDict()
        od[self._get_rev()] = value
        self.data[uid] = od
        # print self.data[uid]
        return self._get_item(uid)

    def delete(self, uid):
        item = self.get(uid)
        return self.put(uid, dict(deleted=item[K_VALUE]))

    def changes(self, since):
        pass

    def _add_change(self, change, uid, rev):
        if change not in [CHANGE_FRESH, CHANGE_UPDATED, CHANGE_DELETED]:
            raise ValueError
        self.changes.append({
            K_UID: K_REV
        })

    def _get_item(self, uid):
        rev = next(reversed(self.data[uid]))
        value = self.data[uid][rev]
        return {K_REV: rev, K_VALUE: value, K_UID: uid}

    def _get_rev(self, rev=None):
        try:
            m = re.match('^(\d+)-', rev)
            num = int(m.group(1)) + 1
        except:
            num = 1

        return str(num) + '-' + str(uuid4())
