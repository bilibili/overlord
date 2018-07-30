#!/usr/bin/env python

import redis
import argparse
import os

from contextlib import contextmanager

def gen_str(n):
    return ''.join(map(lambda xx: (hex(ord(xx))[2:]), os.urandom(n)))


def gen_items(prefix, n):
    return [
        "_overlord-%s-%s-%010d" % (prefix, gen_str(8), num) for num in range(n)
    ]


def parse_ip_port(addr):
    asp = addr.split(":")
    return (asp[0], int(asp[1]))


def dial(expect):
    ip, port = parse_ip_port(expect)
    rc = redis.StrictRedis(host=ip, port=port)
    return rc

@contextmanager
def del_keys(rc, keys):
    try:
        yield
    finally:
        epipe = rc.pipeline(transaction=False)
        for key in keys:
            epipe.delete(key)
        epipe.execute()


def check_vals(expect_rc, check_rc, keys, vals):
    epipe = expect_rc.pipeline(transaction=False)
    for key, val in zip(keys, vals):
        epipe.set(key, val, ex=10)
    epipe.execute()

    cpipe = check_rc.pipeline(transaction=False)
    for key in keys:
        epipe.get(key)
    for i,val in enumerate(epipe.execute()):
        assert vals[i] == val


def run_check(check, expect, n=1024):
    keys = gen_items("keys", n)
    vals = gen_items("vals", n)

    expect_rc = dial(expect)
    check_rc = dial(check)

    with del_keys(expect_rc, keys):
        check_vals(expect_rc, check_rc, keys, vals)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("check",  help="address need to be checked.")
    parser.add_argument(
        "expect",
        help=
        "expect validate address. command will be send to this address first.")
    parser.add_argument("-k", "--keys", default=1024, help="range range of keys")
    opt = parser.parse_args()
    check = opt.check
    expect = opt.check
    run_check(check, expect, n=opt.keys)


if __name__ == "__main__":
    main()
