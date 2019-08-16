package main

import (
    "errors"
    "io"
    "io/ioutil"
    "os"
    "sync"
)

var (
    ErrFileNotOpen = errors.New("file not open")
)

type logFile struct {
    f *os.File
    c sync.RWMutex
}

func (lf *logFile) read(f func(r io.Reader) error) error {
    lf.c.RLock()
    defer lf.c.RUnlock()

    if lf.f != nil {
        lf.f.Seek(0, 0)
        return f(lf.f)
    }

    return ErrFileNotOpen
}

func (lf *logFile) append(r io.Reader) (n int, err error) {
    lf.c.Lock()
    defer lf.c.Unlock()

    if lf.f != nil {
        b, err := ioutil.ReadAll(r)
        if err != nil {
            return 0, err
        }
        return lf.f.Write(b)
    }

    return 0, ErrFileNotOpen
}
