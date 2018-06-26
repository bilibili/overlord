#!/bin/bash

set -e

PTH=`pwd`
echo "pwd is :$PTH"

echo "Start to test overlord package"
for pkg in `go list ./...|grep -v cmd`; do
    rm -f cover.out || echo "first try to remove"
    go test --coverprofile=cover.out $pkg && go tool cover -func=cover.out
done
# cd $PTH
#find . -name "cover.out" | xargs -I{} cat {} >> proj.out
