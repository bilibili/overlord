#!/usr/bin/env python

try:
    from gevent.monkey import patch_all
    patch_all()
    from gevent.pool import Pool

    from fakeredis import FakeStrictRedis
    from redis import StrictRedis
except ImportError:
    print("""ERROR: you are running within a bad dependencies environment.
You may run the follows commands to fixed it:

    pip install fakeredis==0.11.0 redis==2.10.6 gevent==1.3.5

""")
    raise

import random
import sys

host = "127.0.0.1"
port = 26379
timeout = 3


def gen_items(prefix, n):
    return ["%s%010d" % (prefix, num) for num in range(n)]


class Cmd(object):
    def __init__(self, fn, *args, **kwargs):
        self.fn = fn
        self.args = args
        self.kwargs = kwargs

    def execute(self):
        rslt = self.fn(*self.args, **self.kwargs)

    def __str__(self):
        return "Cmd<fn=%s, args=%s, kwargs=%s>" % (self.fn.__name__, self.args,
                                                   self.kwargs)

    def __repr__(self):
        return self.__str__()


def append_cmd(l, is_write, rcfn, fakefn, *args, **kwargs):
    l.append((is_write, Cmd(rcfn, *args, **kwargs), Cmd(
        fakefn, *args, **kwargs)))


def gen_cmds(fake, rc, keys, vals):
    """ Checked Commands list:
    STRING Commands: GET(1) SET(2) MGET(n) MSET(2n)
    HASH Commands: HGET(2) HGETALL(1) HMGET(2n) HSET(3) HMSET(1+2n)
    SET Commands: SCARD(1) SMEMBERS(1) SISMEMBER(2) SADD(2)
    ZSET Commands: ZCOUNT(1) ZCARD(1) ZRANGE(3) ZADD(3)
    LIST Commands: LLEN(1) LPOP(1) LPUSH(2) RPOP(1)
    HYPERLOGLOG Commands: PFCOUNT(1) PFADD
    """
    # string
    string_cmd_pairs = []
    append_cmd(string_cmd_pairs, False, rc.get, fake.get, keys[0])
    append_cmd(string_cmd_pairs, True, rc.set, fake.set, keys[1], vals[1])
    append_cmd(string_cmd_pairs, False, rc.mget, fake.mget, keys[2], keys[3])
    append_cmd(string_cmd_pairs, True, rc.mset, fake.mset, {
        keys[4]: vals[4],
        keys[5]: vals[5]
    })
    yield string_cmd_pairs

    # hash
    hash_cmd_pairs = []
    append_cmd(hash_cmd_pairs, True, rc.hset, fake.hset, keys[100], keys[10],
               vals[10])
    append_cmd(hash_cmd_pairs, True, rc.hset, fake.hset, keys[100], keys[11],
               vals[11])
    append_cmd(hash_cmd_pairs, True, rc.hset, fake.hset, keys[100], keys[12],
               vals[12])

    append_cmd(hash_cmd_pairs, True, rc.hset, fake.hset, keys[101], keys[10],
               vals[10])
    append_cmd(hash_cmd_pairs, True, rc.hset, fake.hset, keys[101], keys[11],
               vals[11])
    append_cmd(hash_cmd_pairs, True, rc.hset, fake.hset, keys[101], keys[12],
               vals[12])

    append_cmd(hash_cmd_pairs, False, rc.hget, fake.hget, keys[100], keys[10])
    append_cmd(hash_cmd_pairs, False, rc.hget, fake.hget, keys[100], keys[11])
    append_cmd(hash_cmd_pairs, False, rc.hget, fake.hget, keys[100], keys[12])
    append_cmd(hash_cmd_pairs, False, rc.hget, fake.hget, keys[101], keys[10])
    append_cmd(hash_cmd_pairs, False, rc.hget, fake.hget, keys[101], keys[11])
    append_cmd(hash_cmd_pairs, False, rc.hget, fake.hget, keys[101], keys[12])

    append_cmd(hash_cmd_pairs, False, rc.hmget, fake.hmget, keys[101],
               [keys[10], keys[11]])
    append_cmd(hash_cmd_pairs, False, rc.hmget, fake.hmget, keys[101],
               [keys[11], keys[12]])
    append_cmd(hash_cmd_pairs, False, rc.hmget, fake.hmget, keys[101],
               [keys[10], keys[12]])
    append_cmd(hash_cmd_pairs, False, rc.hmget, fake.hmget, keys[101],
               [keys[13]])

    append_cmd(hash_cmd_pairs, True, rc.hmset, fake.hmset, keys[100], {
        keys[14]: vals[14],
        keys[15]: vals[15]
    })
    append_cmd(hash_cmd_pairs, True, rc.hmset, fake.hmset, keys[100], {
        keys[14]: vals[15],
        keys[15]: vals[14]
    })
    append_cmd(hash_cmd_pairs, True, rc.hmset, fake.hmset, keys[101], {
        keys[14]: vals[14],
        keys[15]: vals[15]
    })
    append_cmd(hash_cmd_pairs, True, rc.hmset, fake.hmset, keys[101], {
        keys[14]: vals[15],
        keys[15]: vals[14]
    })

    append_cmd(hash_cmd_pairs, False, rc.hgetall, fake.hgetall, keys[100])
    append_cmd(hash_cmd_pairs, False, rc.hgetall, fake.hgetall, keys[101])
    append_cmd(hash_cmd_pairs, False, rc.hgetall, fake.hgetall, keys[102])

    yield hash_cmd_pairs

    # set
    set_cmd_pairs = []
    append_cmd(set_cmd_pairs, True, rc.sadd, fake.sadd, keys[200], vals[20])
    append_cmd(set_cmd_pairs, True, rc.sadd, fake.sadd, keys[200], vals[21])
    append_cmd(set_cmd_pairs, True, rc.sadd, fake.sadd, keys[200], vals[22])

    append_cmd(set_cmd_pairs, True, rc.sadd, fake.sadd, keys[201], vals[20])
    append_cmd(set_cmd_pairs, True, rc.sadd, fake.sadd, keys[201], vals[21])

    append_cmd(set_cmd_pairs, False, rc.smembers, fake.smembers, keys[201])
    append_cmd(set_cmd_pairs, False, rc.smembers, fake.smembers, keys[200])
    append_cmd(set_cmd_pairs, False, rc.smembers, fake.smembers, keys[202])

    append_cmd(set_cmd_pairs, False, rc.sismember, fake.sismember, keys[200],
               vals[20])
    append_cmd(set_cmd_pairs, False, rc.sismember, fake.sismember, keys[200],
               vals[21])
    append_cmd(set_cmd_pairs, False, rc.sismember, fake.sismember, keys[200],
               vals[23])

    append_cmd(set_cmd_pairs, False, rc.scard, fake.scard, keys[200])
    append_cmd(set_cmd_pairs, False, rc.scard, fake.scard, keys[201])
    yield set_cmd_pairs

    # zset
    zset_cmd_pairs = []
    append_cmd(zset_cmd_pairs, True, rc.zadd, fake.zadd, keys[300], 1.1,
               vals[30], 2.2, vals[40])
    append_cmd(zset_cmd_pairs, False, rc.zcount, fake.zcount, keys[300], 0.1,
               1.5)
    append_cmd(zset_cmd_pairs, False, rc.zrange, fake.zrange, keys[300], 0, 20)
    append_cmd(zset_cmd_pairs, False, rc.zcard, fake.zcard, keys[300])
    yield zset_cmd_pairs

    list_cmd_pairs = []
    append_cmd(list_cmd_pairs, True, rc.lpush, fake.lpush, keys[400], vals[40])
    append_cmd(list_cmd_pairs, False, rc.llen, fake.llen, keys[400])
    append_cmd(list_cmd_pairs, False, rc.rpop, fake.rpop, keys[400])
    append_cmd(list_cmd_pairs, False, rc.lpop, fake.lpop, keys[400])
    yield list_cmd_pairs

    hyperloglog_cmd_pairs = []
    append_cmd(hyperloglog_cmd_pairs, True, rc.pfadd, fake.pfadd, keys[500], vals[50])
    append_cmd(hyperloglog_cmd_pairs, True, rc.pfadd, fake.pfadd, keys[500], vals[51])
    append_cmd(hyperloglog_cmd_pairs, True, rc.pfadd, fake.pfadd, keys[500], vals[52])

    append_cmd(hyperloglog_cmd_pairs, True, rc.pfcount, fake.pfcount, keys[500])
    append_cmd(hyperloglog_cmd_pairs, True, rc.pfcount, fake.pfcount, keys[500])
    yield hyperloglog_cmd_pairs

def run_check(is_write, cmd1, cmd2):
    if is_write:
        cmd1.execute()
        cmd2.execute()
    else:
        rslt1 = None
        rslt2 = None
        try:
            rslt1 = cmd1.execute()
            rslt2 = cmd2.execute()
            assert rslt1 == rslt2
        except AssertionError as e:
            print("assert execute %s == %s" % (cmd1, cmd2))
            print("\tassert result %s == %s" % (rslt1, rslt2))
            raise


def check(cmds_list_all, iround=100):
    for cmds_list in cmds_list_all:
        for _ in range(iround):
            if cmds_list:
                is_write, cmd1, cmd2 = random.choice(cmds_list)
                run_check(is_write, cmd1, cmd2)

def delete_all(rc, keys):
    pipe = rc.pipeline(transaction=False)
    for key in keys:
        rc.delete(key)
    pipe.execute()

def run(i):
    keys = gen_items("keys-%04d" % (i, ), 501)
    vals = gen_items("vals-%04d" % (i, ), 60)
    rc = StrictRedis(host=host, port=port)
    fake = FakeStrictRedis()
    cmds_list = list(gen_cmds(fake, rc, keys, vals))
    check(cmds_list)
    delete_all(rc, keys)


def main():
    global host, port
    if len(sys.argv) == 3:
        host = sys.argv[1].strip()
        port = int(sys.argv[2].strip())
    elif len(sys.argv) == 2:
        port = int(sys.argv[1].strip())

    pool = Pool(20)
    list(pool.map(run, range(100)))


if __name__ == "__main__":
    main()
