package slowlog

import (
	"encoding/json"
	"fmt"
	"net/http"
	"overlord/proxy/proto"
)

// showlog will show slowlog to http
func showlog(w http.ResponseWriter, _req *http.Request) {
	storeLock.RLock()
	var slogs = make([]*proto.SlowlogEntries, len(storeMap))
	idx := 0
	for _, s := range storeMap {
		slogs[idx] = s.Reply()
		idx++
	}
	storeLock.RUnlock()

	encoder := json.NewEncoder(w)
	err := encoder.Encode(slogs)
	if err != nil {
		http.Error(w, fmt.Sprintf("%s", err), http.StatusInternalServerError)
	}
}

// registerSlowlogHTTP will register slowlog by /slowlog
func registerSlowlogHTTP() {
	http.HandleFunc("/slowlog", showlog)
}
