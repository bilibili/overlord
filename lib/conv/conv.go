package conv

import (
	"strconv"
)

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

// UpdateToLower will convert to lower case
func UpdateToLower(src []byte) {
	const step = byte('a') - byte('A')
	for i := range src {
		if src[i] >= 'A' && src[i] <= 'Z' {
			src[i] += step
		}
	}
}

// UpdateToUpper will convert to lower case
func UpdateToUpper(src []byte) {
	const step = byte('a') - byte('A')
	for i := range src {
		if src[i] >= 'a' && src[i] <= 'z' {
			src[i] -= step
		}
	}
}
