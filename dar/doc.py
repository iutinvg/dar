from collections import namedtuple, OrderedDict
from functools import partial
import hashlib

from asciitree import LeftAligned

from . import exceptions

# Result is operations result
RevisionOld = namedtuple('RevisionOld', 'uid value rev deleted parent')
Rev = partial(RevisionOld, deleted=False)

Revision = namedtuple('Revision', 'value deleted parent')


# agreement for code below:
# `rev` is a string which is revision ID
# `revision` is an object with `rev` property where its ID is stored


class Document(OrderedDict):
    def __init__(self, *args, **kwargs):
        super(Document, self).__init__(*args, **kwargs)
        self.winner = None
        self.conflicts = set()

    def put(self, value, rev=None):
        if rev is None:
            if len(self):
                exceptions.DataError('multiple roots is not allowed')
        elif rev not in self:
            raise exceptions.NotFoundError('unknown revision')

        new_rev = self.new_rev(value, rev)

        revision = Revision(
            value=value,
            deleted=False,
            parent=rev
        )
        self[new_rev] = revision
        self.update_winner(self, new_rev, revision)
        return revision

    def update_winner(self, new_rev, revision):
        leafs = set(self.conflicts)
        if self.winner:
            leafs.add(self.winner)

        if revision.parent in leafs:
            leafs.remove(revision.parent)
        leafs.add(new_rev)

        winner = max((self._path_length(l), l) for l in leafs)
        self.winner = winner[1]

        leafs.remove(self.winner)
        self.conflicts = leafs

        return self.winner

    def _path_length(self, rev):
        l = 1
        if rev in self:
            revision = self[rev]
            while revision.parent:
                l += 1
                revision = self[revision.parent]
        return l

    def new_rev(self, value, rev):
        return new_rev(value, rev, prefix=lambda: str(len(self) + 1))
        # return new_rev(value, rev)

    def __str__(self):
        d = self.__render_tree(None)
        tr = LeftAligned()
        return tr(d)

    def __render_tree(self, rev):
        d = OrderedDict()
        for k, v in self.iteritems():
            if v.parent != rev:
                continue
            # print k
            d[k] = self.__render_tree(k)
        return d


def new_rev(value, rev, prefix=None):
    p = (prefix() + '-') if prefix else ''
    return p + hashlib.sha1(str(rev) + str(value)).hexdigest()
