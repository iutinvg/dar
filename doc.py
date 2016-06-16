from collections import namedtuple, OrderedDict
from functools import partial
import hashlib


# Result is operations result
Revision = namedtuple('Revision', 'uid value rev deleted parent seq')
Rev = partial(Revision, deleted=False, seq=0)


class Document(OrderedDict):
    def new_rev(self, value, rev):
        return new_rev(value, rev, prefix=lambda: str(len(self) + 1))
        # return new_rev(value, rev)

    # def __str__(self):
    #     pairs = []
    #     for k, v in self.iteritems():
    #         pairs.append("\t{:.5}... : {:.5}...".format(k, v.value))
    #     return "{}:\n{}".format(next(self.itervalues())['uid'], "\n".join(pairs))


def new_rev(value, rev, prefix=None):
    p = (prefix() + '-') if prefix else ''
    return p + hashlib.sha1(str(rev) + str(value)).hexdigest()
