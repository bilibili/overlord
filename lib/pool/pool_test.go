package pool_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/felixhao/overlord/lib/pool"
)

type poolTestConn struct {
	t  *testing.T
	id int
}

func newPoolTestConn(t *testing.T, id int) (pool.Conn, error) {
	return &poolTestConn{t: t, id: id}, nil
}

func (c *poolTestConn) Close() error {
	// c.t.Logf("close by id: %d", c.id)
	return nil
}

type poolDialer struct {
	mu     sync.Mutex
	t      *testing.T
	dialed int
}

func (d *poolDialer) dial() (pool.Conn, error) {
	d.mu.Lock()
	d.dialed++
	d.mu.Unlock()

	c, err := newPoolTestConn(d.t, d.dialed)
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (d *poolDialer) check(message string, p *pool.Pool, dialed, open int) {
	d.mu.Lock()
	if d.dialed != dialed {
		d.t.Errorf("%s: dialed=%d, want %d", message, d.dialed, dialed)
	}
	if active := p.ActiveCount(); active != open {
		d.t.Errorf("%s: active=%d, want %d", message, active, open)
	}
	d.mu.Unlock()
}

func newPool(t *testing.T, dial func() (pool.Conn, error), active, idle int, it time.Duration) *pool.Pool {
	d := pool.PoolDial(dial)
	i := pool.PoolIdle(idle)
	a := pool.PoolActive(active)
	ti := pool.PoolIdleTimeout(it)
	p := pool.NewPool(d, i, a, ti)
	return p
}

func TestPoolReuse(t *testing.T) {
	d := &poolDialer{t: t}
	p := newPool(t, d.dial, 0, 2, 0)

	for i := 0; i < 10; i++ {
		c1 := p.Get()
		c1.Close()
		c2 := p.Get()
		c2.Close()
		p.Put(c1, false)
		p.Put(c2, false)
	}
	d.check("before close", p, 2, 2)
	p.Close()
	d.check("after close", p, 2, 0)
}

func TestPoolMaxIdle(t *testing.T) {
	d := &poolDialer{t: t}
	p := newPool(t, d.dial, 0, 2, 0)

	for i := 0; i < 10; i++ {
		c1 := p.Get()
		c1.Close()
		c2 := p.Get()
		c2.Close()
		c3 := p.Get()
		c3.Close()
		p.Put(c1, false)
		p.Put(c2, false)
		p.Put(c3, false)
	}
	d.check("before close", p, 12, 2)
	p.Close()
	d.check("after close", p, 12, 0)
}

func TestPoolPut(t *testing.T) {
	d := &poolDialer{t: t}
	p := newPool(t, d.dial, 0, 2, 0)
	defer p.Close()

	c := p.Get()
	c.Close()
	c.Close()
	p.Put(c, true)

	c = p.Get()
	c.Close()
	c.Close()
	p.Put(c, true)

	c = p.Get()
	c.Close()
	// c.Close()
	p.Put(c, false)

	d.check(".", p, 3, 1)
}

func TestPoolTimeout(t *testing.T) {
	d := &poolDialer{t: t}
	p := newPool(t, d.dial, 0, 2, time.Second)
	defer p.Close()

	c := p.Get()
	c.Close()
	p.Put(c, false)

	d.check("1", p, 1, 1)

	c = p.Get()
	c.Close()
	p.Put(c, false)

	d.check("2", p, 1, 1)

	time.Sleep(2 * time.Second)

	c = p.Get()
	c.Close()
	p.Put(c, false)

	d.check("3", p, 2, 1)
}

func TestPoolConcurren(t *testing.T) {
	d := &poolDialer{t: t}
	p := newPool(t, d.dial, 0, 2, time.Second)
	defer p.Close()

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			c := p.Get()
			go func() {
				time.Sleep(100 * time.Millisecond) // NOTE: Ensure that the connection has not been put back to the pool
				c.Close()
				p.Put(c, false)
				wg.Done()
			}()
		}()
	}
	wg.Wait()

	d.check(".", p, 5, 2)
}

func TestPoolBorrowCheck(t *testing.T) {
	d := &poolDialer{t: t}
	p := newPool(t, d.dial, 0, 2, time.Second)
	defer p.Close()
	p.TestOnBorrow = func(pool.Conn, time.Time) error { return errors.New("BLAH") }

	for i := 0; i < 10; i++ {
		c := p.Get()
		c.Close()
		p.Put(c, false)
	}
	d.check(".", p, 10, 1)
}

func TestPoolMaxActive(t *testing.T) {
	d := &poolDialer{t: t}
	p := newPool(t, d.dial, 2, 2, time.Second)
	defer p.Close()

	c1 := p.Get()
	c1.Close()
	c2 := p.Get()
	c2.Close()

	d.check("1", p, 2, 2)

	c3 := p.Get()
	if err := c3.Close(); err != pool.ErrPoolExhausted {
		t.Errorf("expected pool exhausted")
	}

	p.Put(c3, false)
	d.check("2", p, 2, 2)

	p.Put(c2, false)
	d.check("3", p, 2, 2)

	c3 = p.Get()
	if err := c3.Close(); err != nil {
		t.Errorf("expected good channel, err=%v", err)
	}
	p.Put(c3, false)

	d.check("4", p, 2, 2)
}

func startGoroutines(t *testing.T, p *pool.Pool, hopeErr error) chan int {
	idCh := make(chan int, 10)
	for i := 0; i < len(idCh); i++ {
		go func() {
			c := p.Get()
			err := c.Close()
			switch v := c.(type) {
			case *poolTestConn:
				idCh <- v.id
			default:
				idCh <- -1
				if err != hopeErr {
					t.Errorf("pool get error:%v, but not:%v", err, hopeErr)
				} else {
					t.Logf("pool get error:%v", err)
				}
			}
			p.Put(c, false)
		}()
	}
	// Wait for goroutines to block.
	time.Sleep(time.Second / 4)
	return idCh
}

func TestWaitPool(t *testing.T) {
	d := &poolDialer{t: t}
	p := newPool(t, d.dial, 1, 1, time.Second)
	defer p.Close()
	p.Wait = true

	c := p.Get()
	idCh := startGoroutines(t, p, nil)
	d.check("before close", p, 1, 1)
	p.Put(c, false)
	for i := 0; i < len(idCh); i++ {
		select {
		case id := <-idCh:
			if id > 0 {
				t.Logf("id always==1: %d", id)
			} else {
				t.Error("id==-1 occur error")
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting for blocked goroutine %d", i)
		}
	}
	d.check("done", p, 1, 1)
}

func TestWaitPoolClose(t *testing.T) {
	d := &poolDialer{t: t}
	p := newPool(t, d.dial, 1, 1, time.Second)
	p.Wait = true

	c := p.Get()
	idCh := startGoroutines(t, p, nil)
	d.check("before close", p, 1, 1)
	p.Close()
	for i := 0; i < len(idCh); i++ {
		select {
		case id := <-idCh:
			if id > 0 {
				t.Logf("id always==1: %d", id)
			} else {
				t.Error("id==-1 occur error")
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for blocked goroutine")
		}
	}
	p.Put(c, false)
	d.check("done", p, 1, 0)
}

func TestWaitPoolDialError(t *testing.T) {
	d := &poolDialer{t: t}
	hopeErr := errors.New("test dial error")
	dial := func() (pool.Conn, error) {
		d.dial()
		return nil, hopeErr
	}
	p := newPool(t, d.dial, 1, 1, time.Second)
	defer p.Close()

	c := p.Get()
	p.Dial = dial
	idCh := startGoroutines(t, p, hopeErr)
	d.check("before close", p, 1, 1)
	p.Put(c, false)

	for i := 0; i < len(idCh); i++ {
		select {
		case id := <-idCh:
			if id == -1 {
				t.Log("id always -1")
			} else {
				t.Errorf("id always -1,but: %d", id)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting for blocked goroutine %d", i)
		}
	}
	d.check("done", p, len(idCh)+1, 1)
}

// Borrowing requires us to iterate over the idle connections, unlock the pool,
// and perform a blocking operation to check the connection still works. If
// TestOnBorrow fails, we must reacquire the lock and continue iteration. This
// test ensures that iteration will work correctly if multiple threads are
// iterating simultaneously.
func TestLocking_TestOnBorrowFails_PoolDoesntCrash(t *testing.T) {
	const count = 100

	// First we'll Create a pool where the pilfering of idle connections fails.
	d := &poolDialer{t: t}
	p := newPool(t, d.dial, count, count, time.Second)
	defer p.Close()
	p.TestOnBorrow = func(c pool.Conn, t time.Time) error {
		return errors.New("no way back into the real world")
	}

	// Fill the pool with idle connections.
	conns := make([]pool.Conn, count)
	for i := range conns {
		conns[i] = p.Get()
	}
	for i := range conns {
		p.Put(conns[i], false)
	}

	// Spawn a bunch of goroutines to thrash the pool.
	var wg sync.WaitGroup
	wg.Add(count)
	for i := 0; i < count; i++ {
		go func() {
			c := p.Get()
			c.Close()
			p.Put(c, false)
			wg.Done()
		}()
	}
	wg.Wait()
	if d.dialed != count*2 {
		t.Errorf("Expected %d dials, got %d", count*2, d.dialed)
	}
}

func BenchmarkPoolGet(b *testing.B) {
	b.StopTimer()
	d := func() (pool.Conn, error) { return &poolTestConn{}, nil }
	p := pool.NewPool()
	p.Dial = d
	p.MaxIdle = 2
	defer p.Close()
	c := p.Get()
	p.Put(c, false)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		c = p.Get()
		p.Put(c, false)
	}
}
