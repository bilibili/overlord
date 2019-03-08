export GO111MODULE=on

test:
	./scripts/codecov.sh

build:
	cd cmd/apicli && go build && cd -
	cd cmd/apiserver && go build && cd -
	cd cmd/balancer && go build && cd -
	cd cmd/executor && go build && cd -
	cd cmd/proxy && go build && cd -
	cd cmd/scheduler && go build && cd -
	cd cmd/anzi && go build && cd -
