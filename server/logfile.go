package main

import (
    "bufio"
    "fmt"
    "os"
    "path/filepath"
    "sync"
    "syscall"
    "time"
)

// 50 Kb
const maxScannerBufferSize = 50 << (10 * 1)

// 500 b
const capScannerBufferSize = 500

// 4 Mb
const rawFileSize = 4 << (10 * 2)
const mergeSpeedRate = time.Second

const flagDefault = os.O_CREATE | os.O_RDWR | os.O_APPEND
const permDefault = 0644

type logFile struct {
    mxFileOps sync.Mutex
    location  string
    lastOps   time.Time
    file      *os.File
    key       uint32
}

func (lf *logFile) close() {
    fmt.Printf("close log, %d\n", lf.key)

    if lf.file != nil {
        lf.file.Close()
    }
}

func (lf *logFile) register(buffer []byte) error {
    if lf.file == nil {
        err := lf.open()
        if err != nil {
            return err
        }
    }

    lf.mxFileOps.Lock()
    defer lf.mxFileOps.Unlock()

    _, err := lf.file.Write(buffer)
    if err != nil {
        return err
    }

    lf.lastOps = time.Now()
    return nil
}

func (lf *logFile) walk(f func(line []byte)) error {
    lf.mxFileOps.Lock()
    defer lf.mxFileOps.Unlock()

    if lf.file == nil {
        err := lf.open()
        if err != nil {
            return err
        }
    }

    lf.lastOps = time.Now()
    lf.file.Seek(0, 0)

    scanner := bufio.NewScanner(lf.file)
    scanner.Buffer(make([]byte, 0, capScannerBufferSize), maxScannerBufferSize)
    for scanner.Scan() {
        f(scanner.Bytes())
    }

    return scanner.Err()
}

func (lf *logFile) open() (err error) {
    fmt.Printf("open log, %d\n", lf.key)
    lf.file, err = os.OpenFile(lf.location, flagDefault, permDefault)
    if e, ok := err.(*os.PathError); ok && e.Err == syscall.ERROR_PATH_NOT_FOUND {

        err := os.MkdirAll(filepath.Dir(lf.location), permDefault)
        if err != nil {
            return err
        }

        lf.file, err = os.OpenFile(lf.location, flagDefault, permDefault)
        if err != nil {
            return err
        }
    }

    return nil
}
