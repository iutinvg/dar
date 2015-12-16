CHANGE_FRESH = 0
CHANGE_UPDATED = 1
CHANGE_DELETED = 2


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
