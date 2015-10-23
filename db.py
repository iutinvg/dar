from uuid import uuid4
import re
import hashlib


CHANGE_FRESH = 'fresh'
CHANGE_UPDATED = 'updated'
CHANGE_DELETED = 'deleted'


class Item(object):
    def __init__(self, uid, rev, value, deleted=False):
        self.uid = uid
        self.rev = rev
        self.value = value
        self.deleted = deleted

    def __str__(self):
        return dict(uid=self.uid, rev=self.rev, value=self.value, deleted=self.deleted)


class Change(object):
    def __init__(self, change, uid, rev, deleted):
        if change not in [CHANGE_FRESH, CHANGE_UPDATED, CHANGE_DELETED]:
            raise ValueError

        self.change = change
        self.uid = uid
        self.rev = rev
        self.deleted = deleted


class Db(object):
    def __init__(self, name=None):
        self.data = []
        self.changes = []
        self.local = {}
        self.name = name

    def get(self, uid, rev=None):
        item = self._get_item(uid, rev)
        if not item or item.deleted:
            raise LookupError
        return item

    def post(self, value):
        new_item = Item(
            uid=str(uuid4()),
            rev=self._get_rev(),
            value=value
        )
        self.data.append(new_item)
        self._add_change(CHANGE_FRESH, new_item)
        return new_item

    def put(self, uid, value):
        item = self._get_item(uid)
        new_item = Item(
            uid=item.uid,
            rev=self._get_rev(item.rev),
            value=value
        )
        self.data.append(new_item)
        self._add_change(CHANGE_UPDATED, new_item)
        return new_item

    def delete(self, uid):
        item = self._get_item(uid)
        new_item = Item(
            uid=item.uid,
            rev=self._get_rev(item.rev),
            value=item.value,
            deleted=True
        )
        self.data.append(new_item)
        self._add_change(CHANGE_DELETED, new_item, new_item.deleted)
        return new_item

    def get_changes(self, since=0):
        return self.changes[since:]

    def get_rev_diff(self, revs):
        result = {}
        for uid in revs:
            for rev in revs[uid]:
                item = self._get_item(uid, rev)
                if item:
                    continue
                if uid not in result:
                    result[uid] = {'missing': []}
                result[uid]['missing'].append(rev)
        return result

    def _add_change(self, change, item, deleted=False):
        c = Change(change, item.uid, item.rev, deleted)
        self.changes.append(c)

    def _get_item(self, uid, rev=None):
        if rev:
            for i in reversed(self.data):
                if i.uid == uid and i.rev == rev:
                    return i
            return None

        for i in reversed(self.data):
            if i.uid == uid:
                return i

        return None

    # generate rev number
    def _get_rev(self, rev=None):
        try:
            m = re.match('^(\d+)-', rev)
            num = int(m.group(1)) + 1
        except:
            num = 1

        return str(num) + '-' + str(uuid4())

    def _conflict(self, uid, parent_rev):
        parent_item = self._get_item(uid, parent_rev)
        latest_item = self._get_item(uid)
        if parent_item.rev != latest_item.rev:
            raise ValueError('conflict')


class Replacation(object):
    def __init__(self, source, target):
        self.source = source
        self.target = target

    def get_replication_uid(self):
        return hashlib.sha1(self.source.name + self.target.name).hexdigest()

    def replicate(self):
        # step 2
        luid = self.get_replication_uid()

        # step 3
        source_seq = self.source.local.get(luid, 0)
        # target_seq = self.target.local.get(luid, 0)

        # step 4
        source_changes = self.source.get_changes(source_seq)
        source_changes_prepared = self.prepare_changes(source_changes)

        # step 5: diff
        self.target.get_rev_diff(source_changes_prepared)

    def prepare_changes(self, changes):
        result = {}
        for c in changes:
            if c.uid not in result:
                result[c.uid] = []

            result[c.uid].append(c.rev)

        return result
