package dir

import (
	"os"
	"path/filepath"

	pkgerr "github.com/pkg/errors"
)

// define errors
var (
	ErrNotFile = pkgerr.New("path must be a File")
	ErrNotDir  = pkgerr.New("path must be a Dir")
)

// GetAbsDir will get the file's dir absolute path.
func GetAbsDir(path string) (absDir string, err error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		err = pkgerr.Wrapf(err, "path get absolute in GetAbsDir")
		return
	}
	absDir = filepath.Dir(absPath)
	return
}

// MkDirAll will create dir as using `mkdir -p`.
func MkDirAll(path string) (err error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		err = pkgerr.Wrapf(err, "path get absolute")
		return
	}

	stat, err := os.Stat(absPath)
	if err != nil {
		if os.IsExist(err) {
			if stat.IsDir() {
				err = nil
				return
			}
			err = pkgerr.Wrapf(err, "check state of path")
			return
		}
		err = nil
	}

	err = os.MkdirAll(absPath, 0755)
	if err != nil {
		err = pkgerr.Wrapf(err, "when mkdirall meet error")
	}
	return
}
