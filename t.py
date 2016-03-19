from __future__ import print_function
from collections import OrderedDict
from itertools import islice
import hashlib


class MyOrderedDict1(OrderedDict):
    def last(self):
        k = next(reversed(self))
        return (k, self[k])

    def f1(self, since):
        return list(enumerate(islice(self.iterkeys(), since, None), since))

    def f2(self, since):
        return list(islice(enumerate(self.iterkeys()), since, None))

    def hashmd5(self, s1, s2):
        return hashlib.md5(str(s1) + str(s2)).hexdigest()

    def hashsha1(self, s1, s2):
        return hashlib.sha1(str(s1) + str(s2)).hexdigest()


class MyOrderedDict2(OrderedDict):
    def last(self):
        out = self.popitem()
        self[out[0]] = out[1]
        return out


class MyOrderedDict3(OrderedDict):
    def last(self):
        k = (list(self.keys()))[-1]
        return (k, self[k])

if __name__ == "__main__":
    from timeit import Timer

    N = 100

    d1 = MyOrderedDict1()
    # for i in range(N):
    #     d1[i] = i

    # print ("d1.f1", d1.f1(97))
    # print ("d1.f2", d1.f2(97))

    # d2 = MyOrderedDict2()
    # for i in range(N):
    #     d2[i] = i

    # print ("d2", d2.last())

    # d3 = MyOrderedDict3()
    # for i in range(N):
    #     d3[i] = i

    # print("d3", d3.last())

    t = Timer("d1.hashmd5('str1', 'str2')", 'from __main__ import d1')
    print ("md5", t.timeit())
    t = Timer("d1.hashsha1('str1', 'str2')", 'from __main__ import d1')
    print ("sha1", t.timeit())

    # t = Timer("d1.f1(0)", 'from __main__ import d1')
    # print ("f1(0)", t.timeit())
    # t = Timer("d1.f2(0)", 'from __main__ import d1')
    # print ("f2(0)", t.timeit())

    # t = Timer("d1.f1(50)", 'from __main__ import d1')
    # print ("f1(50)", t.timeit())
    # t = Timer("d1.f2(50)", 'from __main__ import d1')
    # print ("f2(50)", t.timeit())

    # t = Timer("d1.f1(95)", 'from __main__ import d1')
    # print ("f1(95)", t.timeit())
    # t = Timer("d1.f2(95)", 'from __main__ import d1')
    # print ("f2(95)", t.timeit())
    # t = Timer("d1.last()", 'from __main__ import d1')
    # print ("OrderedDict1", t.timeit())
    # t = Timer("d2.last()", 'from __main__ import d2')
    # print ("OrderedDict2", t.timeit())
    # t = Timer("d3.last()", 'from __main__ import d3')
    # print ("OrderedDict3", t.timeit())

    # t = Timer("d3.boo()", 'from __main__ import d3')
    # print ("OrderedDict3", t.timeit())
