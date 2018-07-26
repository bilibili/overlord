#!/bin/env python
import redis
import sys

host = "127.0.0.1"
port = 26379
timeout = 3

def gen_items(prefix, n):
    return ["%s%010d" % (prefix, num) for num in range(n)]

def check_set_get_cmd(step, keys, vals):
    rc = redis.StrictRedis(host=host, port=port, socket_timeout=timeout)
    iter_count = len(keys) / step
    for idx in range(iter_count):
        begin = idx * step
        end  = idx * step + step
        for i in range(begin, end):
            rc.set(keys[i], vals[i])

        for i in range(begin, end):
            val = rc.get(keys[i]).encode("utf8")
            assert val == vals[i], "Error Validate set get"


def check_mset_mget_cmd(key_count,step, keys, vals):
    rc = redis.StrictRedis(host=host, port=port, socket_timeout=timeout)
    iter_count = len(keys) / (key_count * step)

    for idx in range(iter_count):
        base = idx * (step * key_count)
        for i in range(step):
            begin = base + i * key_count
            end = base + (i+1) * key_count

            offsets = [offset for offset in range(begin,end)]
            subkeys = [keys[o] for o in offsets]
            subvals = [vals[o] for o in offsets]

            mapping = dict(zip(subkeys, subvals))
            rc.mset(mapping)
            rvals = [x.encode("utf8") for x in rc.mget(subkeys)]
            assert rvals == subvals, "Error validate mset mget"

def check_hmset_hmget_cmd(key_count, step, hashs, keys, vals):
    rc = redis.StrictRedis(host=host, port=port, socket_timeout=timeout)
    iter_count = len(keys) / (key_count * step)

    for idx in range(iter_count):
        base = idx * (step * key_count)

        for i in range(step):
            begin = base + i * key_count
            end = base + (i+1) * key_count
            hash_idx = idx * step + i

            offsets = [offset for offset in range(begin,end)]
            subkeys = [keys[o] for o in offsets]
            subvals = [vals[o] for o in offsets]

            mapping = dict(zip(subkeys, subvals))
            rc.hmset(hashs[hash_idx], mapping)
            rvals = rc.hmget(hashs[hash_idx],subkeys)
            assert rvals == subvals, "Error validate mset mget"


def main():
    global host, port
    if len(sys.argv) > 1:
        host = sys.argv[1].strip()
        port = int(sys.argv[2].strip())

    keys = gen_items("keys-", 1000)
    vals = gen_items("vals-",1000)
    check_set_get_cmd(10, keys, vals)
    check_mset_mget_cmd(10, 10, keys, vals)

    hashs = gen_items("hash-",100)
    check_hmset_hmget_cmd(10, 10, hashs, keys, vals)

if __name__ == "__main__":
    main()
v
