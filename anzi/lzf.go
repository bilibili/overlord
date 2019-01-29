// COPY from https://github.com/vrischmann/rdbtools/blob/master/lzf.go
package anzi

func lzfDecompress(data []byte, ulen int64) []byte {
	output := make([]byte, ulen)

	if len(data) <= 0 {
		return output
	}

	i := uint32(0)
	o := uint32(0)

	for i < uint32(len(data)) {
		var ctrl = uint32(data[i])
		i++
		if ctrl < 32 {
			copy(output[o:], data[i:i+ctrl+1])
			i += ctrl + 1
			o += ctrl + 1
		} else {
			var length = uint32(ctrl >> 5)
			if length == 7 {
				length += uint32(data[i])
				i++
			}

			var ref = uint32(o) - uint32(ctrl&0x1F<<8) - uint32(data[i]) - 1

			i++
			for j := uint32(0); j < length+2; j++ {
				output[o] = output[ref]
				ref++
				o++
			}
		}
	}

	return output
}
