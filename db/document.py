from collections import OrderedDict
import hashlib
import uuid


class Document(object):
    def __init__(self, value):
        self.revs = OrderedDict()
        self.update(value)

    def value(self, rev=None):
        if rev:
            return self.revs[rev]
        return self.revs.values()[-1]

    def update(self, value):
        current_rev = self.revision()
        new_rev = self._generate_rev(current_rev, value)
        self.revs[new_rev] = value

    def revision(self):
        try:
            return self.revs.keys()[-1]
        except:
            return hashlib.sha1(str(uuid.uuid4())).hexdigest()

    def _generate_rev(self, current_rev, value):
        return hashlib.sha1(current_rev + value).hexdigest()

    # def __init__(self, uid, rev, value, deleted=False):
    #     self.uid = uid
    #     self.rev = rev
    #     self.value = value
    #     self.deleted = deleted
    #     self.revs_info = []

    # def __str__(self):
    #     return str(self.to_dict())

    # def to_dict(self):
    #     return dict(
    #         uid=self.uid,
    #         rev=self.rev,
    #         value=self.value,
    #         deleted=self.deleted,
    #         revs_info=self.revs_info,
    #     )
