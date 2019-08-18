package main

import (
    "bufio"
    "compress/gzip"
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
    zero     *os.File
    key      uint32
}

func (lf *logFile) close() {
    if lf.zero != nil {
        lf.mx.Lock()
        defer lf.mx.Unlock()

        lf.zero.Close()
    }
}

func (lf *logFile) since(d time.Duration) bool {
    if lf.zero != nil {
        return time.Since(lf.lastOps) > d
    }

    return false
}

func (lf *logFile) register(buffer []byte) error {
    lf.mx.Lock()
    defer lf.mx.Unlock()

    if lf.zero == nil {
        err := lf.openZero()
        if err != nil {
            return err
        }
    }

    _, err := lf.zero.Write(buffer)
    if err != nil {
        return err
    }

    lf.lastOps = time.Now()
    return nil
}

func (lf *logFile) pathByK(k int) string {
    if k == 0 {
        return filepath.Join(lf.location, "0.log")
    }

    return filepath.Join(lf.location, fmt.Sprintf("%d.gz.log", k))
}

func (lf *logFile) walkByK(k int, f func(line []byte)) error {
    lf.mx.RLock()
    defer lf.mx.RUnlock()

    reader, err := os.Open(lf.pathByK(k))
    if err != nil {
        return err
    }
    defer reader.Close()

    var scanner *bufio.Scanner
    if k > 0 {
        gzReader, err := gzip.NewReader(reader)
        if err != nil {
            return err
        }
        scanner = bufio.NewScanner(gzReader)
    } else  {
        scanner = bufio.NewScanner(reader)
    }

    scanner.Buffer(make([]byte, 0, capScannerBufferSize), maxScannerBufferSize)
    for scanner.Scan() {
        f(scanner.Bytes())
    }

    return scanner.Err()
}

func (lf *logFile) openZero() (err error) {
    p := lf.pathByK(0)
    lf.zero, err = os.OpenFile(p, flagDefault, permDefault)
    if e, ok := err.(*os.PathError); ok && e.Err == syscall.ERROR_PATH_NOT_FOUND {

        err := os.MkdirAll(filepath.Dir(p), permDefault)
        if err != nil {
            return err
        }

        lf.zero, err = os.OpenFile(p, flagDefault, permDefault)
        if err != nil {
            return err
        }
    }

    return nil
}
