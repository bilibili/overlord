import shlex
import subprocess
import sys
import os
import functools
import time


def timeit(f):
    @functools.wraps(f)
    def inner(*args, **kwargs):
        before = time.time()
        try:
            f(*args, **kwargs)
        except Exception:
            raise
        finally:
            after = time.time()
            print("elapsed %f seconds" % (after - before,))

    return inner


@timeit
def check_gen(cwd):
    arr = list(os.listdir(os.path.join(cwd, "corpus")))
    if arr:
        return
    print("generate corpus ing...")
    out = subprocess.check_call(
        shlex.split("go run ./gen/main.go -out ./corpus/"), cwd=cwd)
    print(out)


@timeit
def check_and_build_fuzz(cwd):
    for item in os.listdir(cwd):
        if item.endswith(".zip"):
            print("find %s and using cache" % (item, ))
            print("if you has change your code, rm %s first" % (item, ))
            return

    print("running go-fuzz-build, that may take a lone time")
    print(
        "WARNING: you must set http_proxy and https_proxy in command line to download https://golang.org/x/net?go-get=1 dependecies"
    )
    print("if not, that may take a long time and fail finally.")
    out = subprocess.check_output(shlex.split("go-fuzz-build"), cwd=cwd)
    print(out)


def main():
    cwd = sys.argv[1]
    check_gen(cwd)
    check_and_build_fuzz(cwd)

    sub = subprocess.Popen(shlex.split("go-fuzz"), cwd=cwd)
    sub.communicate()


if __name__ == '__main__':
    main()
