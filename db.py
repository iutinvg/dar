from uuid import uuid4
import re


CHANGE_FRESH = 'fresh'
CHANGE_UPDATED = 'updated'
CHANGE_DELETED = 'deleted'


class Item(object):
    def __init__(self, uid, rev, value):
        self.uid = uid
        self.rev = rev
        self.value = value
        self.deleted = False


class Db(object):
    def __init__(self):
        self.data = []
        self.changes = []

    def get(self, uid):
        item = self._get_item_by_uid(uid)
        if not item or item.deleted:
            raise LookupError
        return item

    def get_by_rev(self, rev):
        item = self._get_item_by_rev(rev)
        if not item:
            raise LookupError
        return item

    def post(self, value):
        new_item = Item(
            uid=str(uuid4()),
            rev=self._get_rev(),
            value=value
        )
        self.data.append(new_item)
        return new_item

    def put(self, uid, value):
        item = self._get_item_by_uid(uid)
        new_item = Item(
            uid=item.uid,
            rev=self._get_rev(item.rev),
            value=value
        )
        self.data.append(new_item)
        return new_item

    def delete(self, uid):
        item = self._get_item_by_uid(uid)
        item.deleted = True
        self.data.append(item)
        return item

    def changes(self, since):
        pass

    # def _add_change(self, change, uid, rev):
    #     if change not in [CHANGE_FRESH, CHANGE_UPDATED, CHANGE_DELETED]:
    #         raise ValueError
    #     self.changes.append({
    #         K_UID: K_REV
    #     })

    def _get_item_by_uid(self, uid):
        for i in reversed(self.data):
            if i.uid == uid:
                return i
        return None

    def _get_item_by_rev(self, rev):
        for i in self.data:
            if i.rev == rev:
                return i
        return None

    def _get_rev(self, rev=None):
        try:
            m = re.match('^(\d+)-', rev)
            num = int(m.group(1)) + 1
        except:
            num = 1

        return str(num) + '-' + str(uuid4())
