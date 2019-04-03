package proxy

type mockPing struct {
	err error
}

func (mp *mockPing) Ping() error {
	return mp.err
}

func (mp *mockPing) Close() error {
	return nil
}

func (mp *mockPing) SetErr(err error) {
	mp.err = err
}

type mockErr struct {
}

func (e *mockErr) Error() string {
	return "mock err"
}
func (e *mockErr) Timeout() bool {
	return true
}
func (e *mockErr) Temporary() bool {
	return true
}
