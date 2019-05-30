package version

import (
	"flag"
	"fmt"
	"os"
)

// Define overlord version consts
const (
	OverlordMajor = 1
	OverlordMinor = 8
	OverlordPatch = 1
)

var showVersion bool
var vstr string

func init() {
	vstr = fmt.Sprintf("%d.%d.%d", OverlordMajor, OverlordMinor, OverlordPatch)
	flag.BoolVar(&showVersion, "version", false, "show version and exit.")
}

// ShowVersion print version if -version flag is seted and return true
func ShowVersion() bool {
	if showVersion {
		fmt.Fprintln(os.Stdout, vstr)
	}
	return showVersion
}
