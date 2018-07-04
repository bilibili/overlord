package conv

import (
	"strconv"
)

const (
	maxCmdLen = 64
)

var (
	charmap [256]byte
)

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

// btoi returns the corresponding value i.
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

// UpdateToLower will convert to lower case
func UpdateToLower(src []byte) {
	for i := range src {
		if c := charmap[src[i]]; c != 0 {
			src[i] = c
		}
	}
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
