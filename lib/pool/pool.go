package pool

import (
	"container/list"
	"errors"
	"io"
	"sync"
	"time"
)

// errors
var (
	ErrPoolExhausted = errors.New("pool: connection exhausted")
	ErrPoolClosed    = errors.New("pool: get on closed")
)

var nowFunc = time.Now

// Conn is connection.
type Conn interface {
	io.Closer
}

// Pool maintains a pool of connections. The application calls the Get method
// to get a connection from the pool and the connection's Close method to
// return the connection's resources to the pool.
type Pool struct {
	// Dial is an application supplied function for creating and configuring a
	// connection.
	Dial func() (Conn, error)
	// TestOnBorrow is an optional application supplied function for checking
	// the health of an idle connection before the connection is used again by
	// the application. Argument t is the time that the connection was returned
	// to the pool. If the function returns an error, then the connection is
	// closed.
	TestOnBorrow func(c Conn, t time.Time) error
	// Maximum number of idle connections in the pool.
	MaxIdle int
	// Maximum number of connections allocated by the pool at a given time.
	// When zero, there is no limit on the number of connections in the pool.
	MaxActive int
	// Close connections after remaining idle for this duration. If the value
	// is zero, then idle connections are not closed. Applications should set
	// the timeout to a value less than the server's timeout.
	IdleTimeout time.Duration
	// If Wait is true and the pool is at the MaxActive limit, then Get() waits
	// for a connection to be returned to the pool before returning.
	Wait bool
	// mu protects fields defined below.
	mu     sync.Mutex
	cond   *sync.Cond
	closed bool
	active int
	// Stack of idleConn with most recently used at the front.
	idle list.List
}

type idleConn struct {
	c Conn
	t time.Time
}

// PoolOption specifies an option for pool.
type PoolOption struct {
	f func(*poolOptions)
}

type poolOptions struct {
	dial        func() (Conn, error)
	active      int
	idle        int
	idleTimeout time.Duration
	wait        bool
}

// PoolDial set pool dial func.
func PoolDial(dial func() (Conn, error)) *PoolOption {
	return &PoolOption{func(po *poolOptions) {
		po.dial = dial
	}}
}

// PoolActive set pool max active.
func PoolActive(active int) *PoolOption {
	return &PoolOption{func(po *poolOptions) {
		po.active = active
	}}
}

// PoolIdle set pool max idle.
func PoolIdle(idle int) *PoolOption {
	return &PoolOption{func(po *poolOptions) {
		po.idle = idle
	}}
}

// PoolIdleTimeout set pool max idle timeout.
func PoolIdleTimeout(it time.Duration) *PoolOption {
	return &PoolOption{func(po *poolOptions) {
		po.idleTimeout = it
	}}
}

// PoolWait set pool max idle timeout.
func PoolWait(w bool) *PoolOption {
	return &PoolOption{func(po *poolOptions) {
		po.wait = w
	}}
}

// NewPool creates a new pool.
func NewPool(pos ...*PoolOption) (p *Pool) {
	p = &Pool{}
	opts := &poolOptions{}
	for _, po := range pos {
		po.f(opts)
	}
	if opts.dial != nil {
		p.Dial = opts.dial
	}
	if opts.dial == nil {
		panic("pool dial cannot nil")
	}
	p.MaxActive = opts.active
	p.MaxIdle = opts.idle
	p.IdleTimeout = opts.idleTimeout
	p.Wait = opts.wait
	return
}

// Get gets a connection. The application must close the returned connection.
// This method always returns a valid connection so that applications can defer
// error handling to the first use of the connection. If there is an error
// getting an underlying connection, then the connection Read, Write, Close,
// and Err methods return that error.
func (p *Pool) Get() Conn {
	c, err := p.get()
	if err != nil {
		return errorConnection{err}
	}
	return c
}

// Put puts a connection into pool idle list, if forceClose=true or
// idle len>maxIdle, then connection will close.
func (p *Pool) Put(c Conn, forceClose bool) error {
	p.mu.Lock()
	if !p.closed && !forceClose {
		p.idle.PushFront(idleConn{t: nowFunc(), c: c})
		if p.idle.Len() > p.MaxIdle {
			c = p.idle.Remove(p.idle.Back()).(idleConn).c
		} else {
			c = nil
		}
	}
	if c == nil {
		if p.cond != nil {
			p.cond.Signal()
		}
		p.mu.Unlock()
		return nil
	}
	p.release()
	p.mu.Unlock()
	return c.Close()
}

// ActiveCount returns the number of active connections in the pool.
func (p *Pool) ActiveCount() int {
	p.mu.Lock()
	active := p.active
	p.mu.Unlock()
	return active
}

// Close releases the resources used by the pool.
func (p *Pool) Close() error {
	p.mu.Lock()
	idle := p.idle
	p.idle.Init()
	p.closed = true
	p.active -= idle.Len()
	if p.cond != nil {
		p.cond.Broadcast()
	}
	p.mu.Unlock()
	for e := idle.Front(); e != nil; e = e.Next() {
		e.Value.(idleConn).c.Close()
	}
	return nil
}

// release decrements the active count and signals waiters. The caller must
// hold p.mu during the call.
func (p *Pool) release() {
	p.active--
	if p.cond != nil {
		p.cond.Signal()
	}
}

// get prunes stale connections and returns a connection from the idle list or
// creates a new connection.
func (p *Pool) get() (Conn, error) {
	p.mu.Lock()
	// Prune stale connections.
	if timeout := p.IdleTimeout; timeout > 0 {
		for i, n := 0, p.idle.Len(); i < n; i++ {
			e := p.idle.Back()
			if e == nil {
				break
			}
			ic := e.Value.(idleConn)
			if ic.t.Add(timeout).After(nowFunc()) {
				break
			}
			p.idle.Remove(e)
			p.release()
			p.mu.Unlock()
			ic.c.Close()
			p.mu.Lock()
		}
	}
	for {
		// Get idle connection.
		for i, n := 0, p.idle.Len(); i < n; i++ {
			e := p.idle.Front()
			if e == nil {
				break
			}
			ic := e.Value.(idleConn)
			p.idle.Remove(e)
			test := p.TestOnBorrow
			p.mu.Unlock()
			if test == nil || test(ic.c, ic.t) == nil {
				return ic.c, nil
			}
			ic.c.Close()
			p.mu.Lock()
			p.release()
		}
		// Check for pool closed before dialing a new connection.
		if p.closed {
			p.mu.Unlock()
			return nil, ErrPoolClosed
		}
		// Dial new connection if under limit.
		if p.MaxActive == 0 || p.active < p.MaxActive {
			dial := p.Dial
			p.active++
			p.mu.Unlock()
			c, err := dial()
			if err != nil {
				p.mu.Lock()
				p.release()
				p.mu.Unlock()
				c = nil
			}
			return c, err
		}
		if !p.Wait {
			p.mu.Unlock()
			return nil, ErrPoolExhausted
		}
		if p.cond == nil {
			p.cond = sync.NewCond(&p.mu)
		}
		p.cond.Wait()
	}
}

type errorConnection struct{ err error }

func (ec errorConnection) Close() error { return ec.err }
