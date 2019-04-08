#!/bin/bash

export GO111MODULE=on

# generate vendor clean go mod files
go mod vendor
mv go.mod go.mod.backup
mv go.sum go.sum.backup

# unset go11module
export GO111MODULE=auto

#ls ci/fuzz/ | xargs -n1 -I"{}" cd {} && go-fuzz-build
ls ci/fuzz/ | xargs -n1 -I"{}" python scripts/fuzz_tools.py ci/fuzz/{}

# defer setting
rm -rf vendor
mv go.mod.backup go.mod
mv go.sum.backup go.sum

export GO111MODULE=on
