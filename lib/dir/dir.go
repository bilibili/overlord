package dir

import (
	"os"
	"path/filepath"

	"overlord/config"

	pkgerr "github.com/pkg/errors"
)

// define errors
var (
	ErrNotFile = pkgerr.New("path must be a File")
	ErrNotDir  = pkgerr.New("path must be a Dir")
)

//IsExists check if the file or dir was exists
func IsExists(path string) (bool, error) {
	if config.GetRunMode() == config.RunModeTest {
		return true, nil
	}

	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}

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
