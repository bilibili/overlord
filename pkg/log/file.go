// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package log

import (
	"fmt"
	stdlog "log"
	"os"
	"path/filepath"
	"time"
)

const (
	dailyRolling = "2006-01-02"
)

type fileHandler struct {
	l *stdlog.Logger

	f        *os.File
	basePath string
	filePath string
	fileFrag string
}

// NewFileHandler new file handler.
func NewFileHandler(basePath string) Handler {
	if _, file := filepath.Split(basePath); file == "" {
		panic("invalid base path")
	}
	l := stdlog.New(nil, "", stdlog.LstdFlags|stdlog.Lshortfile)
	f := &fileHandler{l: l, basePath: basePath}
	if err := f.roll(); err != nil {
		panic(err)
	}
	return f
}

func (r *fileHandler) Log(lv Level, msg string) {
	_ = r.roll()
	_ = r.l.Output(6, fmt.Sprintf("[%s] %s", lv, msg))
}

func (r *fileHandler) Close() error {
	if r.f != nil {
		return r.f.Close()
	}
	return nil
}

func (r *fileHandler) roll() error {
	suffix := time.Now().Format(dailyRolling)
	if r.f != nil {
		if suffix == r.fileFrag {
			return nil
		}
		r.f.Close()
		r.f = nil
	}
	r.fileFrag = suffix
	r.filePath = fmt.Sprintf("%s.%s", r.basePath, r.fileFrag)

	if dir, _ := filepath.Split(r.basePath); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0777); err != nil {
			return err
		}
	}
	f, err := os.OpenFile(r.filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	r.f = f
	r.l.SetOutput(f)
	return nil
}
