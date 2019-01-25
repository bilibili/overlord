package anzi

// MigrateProc is the process for anzi.
type MigrateProc struct {
	cfg      *MigrateConfig
	barrierC chan struct{}
}

type BackEnd struct {
	CacheType string
}

// Instance is the struct for instance node
type Instance struct {
	Addr   string
	Role   string
	Weight int
	Slots  []*Slot
}

func (inst *Instance) SyncRDB(b *chan struct{}, output chan string) error {
	return nil
}

// Slot is the struct of slot
type Slot struct {
	Begin, End int
}

// Migrate start new migrate process
func (m *MigrateProc) Migrate() error {
	// 1. barrier run syncRDB
	// 2. parsed rdb done then send notify to barier chan
	// 3. trying to receive more command and send back replconf size
	// 4. dispatch commands into cluster backend(for more, in copy model)

	return nil
}
