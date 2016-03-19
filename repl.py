import hashlib


class Repl(object):
    def __init__(self, source, target):
        self.source = source
        self.target = target
        self.uid = self.get_uid()

    def get_uid(self):
        return hashlib.sha1(self.source.name + self.target.name).hexdigest()

    def get_last_seq(self):
        return self.target.local_get(self.uid)

    def replicate(self):
        seq = self.get_last_seq()

        grouped = self.source.changes_get_grouped(seq)
        diff = self.target.changes_get_diff(grouped)

        results = self.get_diff_docs(diff)

        self.target.put_bulk(results)

        self.target.local_put(self.uid, self.source.changes_get_size())

    def get_diff_docs(self, diff):
        for uid, l in diff.iteritems():
            for rev in l:
                yield self.source.get(uid, rev)
