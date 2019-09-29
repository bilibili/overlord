package version

import (
	"flag"
	"fmt"
	"os"
)

// Define overlord version consts
const (
	OverlordMajor = 1
	OverlordMinor = 9
	OverlordPatch = 0
)

var (
	showVersion bool
	vstr        string
	vbytes      []byte
)

func init() {
	vstr = fmt.Sprintf("%d.%d.%d", OverlordMajor, OverlordMinor, OverlordPatch)
	vbytes = []byte(vstr)
	flag.BoolVar(&showVersion, "version", false, "show version and exit.")
}

// ShowVersion print version if -version flag is seted and return true
func ShowVersion() bool {
	if showVersion {
		fmt.Fprintln(os.Stdout, vstr)
	}
	return showVersion
}

// Bytes return version bytes
func Bytes() []byte {
	return vbytes
}

// Str is the formatted version string
func Str() string {
	return vstr
}
