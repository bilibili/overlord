package cluster

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	_slotDemo = "6b22f87b78cdb181f7b9b1e0298da177606394f7 172.17.0.2:7003@17003 slave 8f02f3135c65482ac00f217df0edb6b9702691f8 0 1532770704000 4 connected\n" +
		"dff2f7b0fbda82c72d426eeb9616d9d6455bb4ff 172.17.0.2:7004@17004 slave 828c400ea2b55c43e5af67af94bec4943b7b3d93 0 1532770704538 5 connected\n" +
		"b1798ba2171a4bd765846ddb5d5bdc9f3ca6fdf3 172.17.0.2:7000@17000 master - 0 1532770705458 1 connected 0-5460\n" +
		"db2dd7d6fbd2a03f16f6ab61d0576edc9c3b04e2 172.17.0.2:7005@17005 slave b1798ba2171a4bd765846ddb5d5bdc9f3ca6fdf3 0 1532770704437 6 connected\n" +
		"828c400ea2b55c43e5af67af94bec4943b7b3d93 172.17.0.2:7002@17002 master - 0 1532770704000 3 connected 10923-16383\n" +
		"8f02f3135c65482ac00f217df0edb6b9702691f8 172.17.0.2:7001@17001 myself,master - 0 1532770703000 2 connected 5461-10922\n"
)

var (
	_masterMap = map[string]struct{}{
		"172.17.0.2:7000": struct{}{},
		"172.17.0.2:7001": struct{}{},
		"172.17.0.2:7002": struct{}{},
	}
)

func TestParseSlot(t *testing.T) {
	ns, err := parseSlots([]byte(_slotDemo))
	assert.NoError(t, err)
	assert.NotNil(t, ns)

	masters := ns.getMasters()
	for _, master := range masters {
		_, ok := _masterMap[master]
		assert.True(t, ok)
	}
}
