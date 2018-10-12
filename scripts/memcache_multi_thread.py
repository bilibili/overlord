from gevent.monkey import patch_all
patch_all()
from gevent.pool import Pool
import memcache
import random

def run(_x):
    gets = ["key_{}".format(x) for x in range(1000)]
    sets = [("key_{}".format(x), x) for x in range(1000)]
    cmds = gets + sets
    mc = memcache.Client(["127.0.0.1:21211"])
    for x in range(100000):
        item = random.choice(cmds)
        if isinstance(item, basestring):
            _value = mc.get(item)
        else:
            key, val = item
            mc.set(key, val, noreply=False)


def main():
    p = Pool(20)
    p.map(run, range(20))

if __name__ == '__main__':
    main()
