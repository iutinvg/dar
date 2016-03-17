from uuid import uuid4
import re
import hashlib
from collections import OrderedDict


CHANGE_FRESH = 'fresh'
CHANGE_UPDATED = 'updated'
CHANGE_DELETED = 'deleted'


class ConflictException(Exception):
    pass


class Item(object):
    def __init__(self, uid, rev, value, deleted=False):
        self.uid = uid
        self.rev = rev
        self.value = value
        self.deleted = deleted
        self.revs_info = []

    def __str__(self):
        return str(self.to_dict())

    def to_dict(self):
        return dict(
            uid=self.uid,
            rev=self.rev,
            value=self.value,
            deleted=self.deleted,
            revs_info=self.revs_info,
        )


class Change(object):
    def __init__(self, change, uid, rev, deleted):
        if change not in [CHANGE_FRESH, CHANGE_UPDATED, CHANGE_DELETED]:
            raise ValueError

        self.change = change
        self.uid = uid
        self.rev = rev
        self.deleted = deleted

    def __unicode__(self):
        return self.uid

    def __str__(self):
        return 'uid: %s; rev: %s; change: %s; deleted: %d' % (self.uid[:6], self.rev[:6], self.change, self.deleted)


class Db(object):
    def __init__(self, name=None):
        self.data = {}
        self.changes = []
        self.local = {}
        self.name = name

    def get(self, uid, rev=None, revs=False):
        items = self.data[uid]
        if rev:
            return items[rev]

        for x in reversed(items):
            item = items[x]
            break

        if item.deleted:
            raise LookupError('not found')

        if revs:
            self._get_history(item)

        return item

    def post(self, value):
        new_item = Item(
            uid=str(uuid4()),
            rev=self._generate_rev_num(),
            value=value
        )
        if new_item.uid not in self.data:
            self.data[new_item.uid] = OrderedDict()

        self.data[new_item.uid][new_item.rev] = new_item
        self._add_change(CHANGE_FRESH, new_item)
        return new_item

    def put(self, uid, value):
        item = self.get(uid)
        new_item = Item(
            uid=item.uid,
            rev=self._generate_rev_num(item.rev),
            value=value
        )
        self.data[uid][new_item.rev] = new_item
        self._add_change(CHANGE_UPDATED, new_item)
        return new_item

    def merge(self, d):
        self._conflict(d['uid'], d['revs_info'])

        new_item = Item(
            uid=d['uid'],
            rev=d['rev'],
            value=d['value']
        )
        self.data[new_item.uid][new_item.rev] = new_item

    def delete(self, uid):
        item = self.get(uid)
        new_item = Item(
            uid=item.uid,
            rev=self._generate_rev_num(item.rev),
            value=item.value,
            deleted=True
        )
        self.data[uid][new_item.rev] = new_item
        self._add_change(CHANGE_DELETED, new_item, new_item.deleted)
        return new_item

    def get_changes(self, since=0):
        return self.changes[since:]

    def get_rev_diff(self, revs):
        result = {}
        for uid in revs:
            for rev in revs[uid]:
                # print '%s / %s' % (uid[:10], rev[:10])
                try:
                    self.get(uid, rev)
                except:
                    if uid not in result:
                        result[uid] = {'missing': []}

                result[uid]['missing'].append(rev)
        return result

    def _add_change(self, change, item, deleted=False):
        c = Change(change, item.uid, item.rev, deleted)
        self.changes.append(c)

    # generate rev number
    def _generate_rev_num(self, rev=None):
        try:
            m = re.match('^(\d+)-', rev)
            num = int(m.group(1)) + 1
        except:
            num = 1

        return str(num) + '-' + str(uuid4())

    def _conflict(self, uid, rev_info):
        if len(rev_info) == 1:
            return

        latest_item = self.get(uid)
        self._get_history(latest_item)

        # print 'remote'
        # print rev_info[1:]
        # print 'local'
        # print list(reversed(latest_item.revs_info))

        if rev_info[1:] != list(reversed(latest_item.revs_info)):
            raise ConflictException

    def _get_history(self, item):
        items = self.data[item.uid]
        for rev in reversed(items):
            item.revs_info.append(rev)


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
        diff = self.target.get_rev_diff(source_changes_prepared)

        # step 6: GET all missing revs
        missing = []
        for uid in diff:
            for rev in diff[uid]['missing']:
                i = self.source.get(uid, rev, revs=True)
                missing.append(i.to_dict())

        # step 7: merge
        for d in missing:
            self.target.merge(d)



    def prepare_changes(self, changes):
        result = {}
        for c in changes:
            if c.uid not in result:
                result[c.uid] = []

            result[c.uid].append(c.rev)

        return result