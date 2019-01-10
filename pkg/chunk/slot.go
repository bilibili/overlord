package chunk

import "fmt"

// Slot is the type which means slot range .e.g.: [9,10] is means contains 9, 10 slot.
type Slot struct {
	Begin int
	End   int
}

func (s *Slot) String() string {
	if s.Begin == s.End {
		return fmt.Sprintf("%d", s.Begin)
	}
	return fmt.Sprintf("%d-%d", s.Begin, s.End)
}
