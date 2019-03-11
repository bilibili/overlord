package proto

// Request request interface.
type Request interface {
	CmdString() string
	Cmd() []byte
	Key() []byte
	Put()
}

// ProxyConn decode bytes from client and encode write to conn.
type ProxyConn interface {
	Decode([]*Message) ([]*Message, error)
	Encode(msg *Message) error
	Flush() error
}

// NodeConn handle Msg to backend cache server and read response.
type NodeConn interface {
	Write(*Message) error
	Read(*Message) error
	Flush() error
	Close() error
	Addr() string
	Cluster() string
}

// Pinger for executor ping node.
type Pinger interface {
	Ping() error
	Close() error
}

// Forwarder is the interface for backend run and process the messages.
type Forwarder interface {
	Forward([]*Message) error
	Close() error
}
