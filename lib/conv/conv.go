package conv

import (
	"bytes"
	"strconv"
)

const (
	minItoa = -128
	maxItoa = 1024 * 1024

	maxCmdLen = 64
)

var (
	itoaOffset [maxItoa - minItoa + 1]uint32
	itoaBuffer string

	charmap [256]byte
)

func init() {
	var b bytes.Buffer
	for i := range itoaOffset {
		itoaOffset[i] = uint32(b.Len())
		b.WriteString(strconv.Itoa(i + minItoa))
	}
	itoaBuffer = b.String()
}

func init() {
	for i := range charmap {
		c := byte(i)
		switch {
		case c >= 'A' && c <= 'Z':
			charmap[i] = c + 'a' - 'A'
		case c >= 'a' && c <= 'z':
			charmap[i] = c
		}
	}
}

// Itoa returns the string representation of i.
func Itoa(i int64) string {
	if i >= minItoa && i <= maxItoa {
		beg := itoaOffset[i-minItoa]
		if i == maxItoa {
			return itoaBuffer[beg:]
		}
		end := itoaOffset[i-minItoa+1]
		return itoaBuffer[beg:end]
	}
	return strconv.FormatInt(i, 10)
}

// Btoi returns the corresponding value i.
func Btoi(b []byte) (int64, error) {
	if len(b) != 0 && len(b) < 10 {
		var neg, i = false, 0
		switch b[0] {
		case '-':
			neg = true
			fallthrough
		case '+':
			i++
		}
		if len(b) != i {
			var n int64
			for ; i < len(b) && b[i] >= '0' && b[i] <= '9'; i++ {
				n = int64(b[i]-'0') + n*10
			}
			if len(b) == i {
				if neg {
					n = -n
				}
				return n, nil
			}
		}
	}
	n, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return 0, err
	}
	return n, nil
}

// ToLower returns a copy of the string s with all Unicode letters mapped to their lower case.
func ToLower(src []byte) []byte {
	var lower [maxCmdLen]byte
	for i := range src {
		if c := charmap[src[i]]; c != 0 {
			lower[i] = c
		}
	}
	return lower[:len(src)]
}
