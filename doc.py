from collections import namedtuple, OrderedDict
from functools import partial
import hashlib


# Result is operations result
Revision = namedtuple('Revision', 'uid value rev deleted parent')
Rev = partial(Revision, deleted=False)


class Document(OrderedDict):
    def __init__(self, *args, **kwargs):
        super(Document, self).__init__(*args, **kwargs)
        self.winner = None
        self.conflicts = set()

    def put(self, value, rev=None):
        if rev:
            if rev not in self:
                raise NotFoundError('unknown revision')

        if len(self):
            if next(reversed(history)) != rev:
                raise DataError('rev not match')
        elif rev:
            raise DataError('rev does not make sense')

        # new_rev = self.rev(value, rev)
        new_rev = self.new_rev(value, rev)
        item = Rev(
            uid=uid,
            value=value,
            rev=new_rev,
            parent=rev,
        )

    def _set_winner(self):
        pass

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
