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
    mx       sync.RWMutex
    location string
    lastOps  time.Time
    writer   *os.File
    key      uint32
}

func (lf *logFile) close() {
    fmt.Printf("close log, %d\n", lf.key)

    if lf.writer != nil {
        lf.writer.Close()
    }
}

func (lf *logFile) register(buffer []byte) error {
    lf.mx.Lock()
    defer lf.mx.Unlock()

    if lf.writer == nil {
        err := lf.open()
        if err != nil {
            return err
        }
    }

    _, err := lf.writer.Write(buffer)
    if err != nil {
        return err
    }

    lf.lastOps = time.Now()
    return nil
}

func (lf *logFile) walk(f func(line []byte)) error {
    lf.mx.RLock()
    defer lf.mx.RUnlock()

    reader, err := os.Open(lf.location)
    if err != nil {
        return err
    }
    defer reader.Close()

    lf.lastOps = time.Now()
    lf.writer.Seek(0, 0)

    scanner := bufio.NewScanner(reader)
    scanner.Buffer(make([]byte, 0, capScannerBufferSize), maxScannerBufferSize)
    for scanner.Scan() {
        f(scanner.Bytes())
    }

    return scanner.Err()
}

func (lf *logFile) open() (err error) {
    fmt.Printf("open log, %d\n", lf.key)
    lf.writer, err = os.OpenFile(lf.location, flagDefault, permDefault)
    if e, ok := err.(*os.PathError); ok && e.Err == syscall.ERROR_PATH_NOT_FOUND {

        err := os.MkdirAll(filepath.Dir(lf.location), permDefault)
        if err != nil {
            return err
        }

        lf.writer, err = os.OpenFile(lf.location, flagDefault, permDefault)
        if err != nil {
            return err
        }
    }

    return nil
}
