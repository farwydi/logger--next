package main

import (
    "bufio"
    "fmt"
    "os"
    "path/filepath"
    "sync"
    "sync/atomic"
    "syscall"
    "time"
)

// 4 Mb
const rawFileSize = 4 << (10 * 2)
const rateWriteSpeed = time.Second

const flagDefault = os.O_CREATE | os.O_RDWR | os.O_APPEND
const permDefault = 0644

type logFile struct {
    mxFileOps sync.Mutex
    mxRAMOps  sync.Mutex
    location  string
    openTime  time.Time
    file      *os.File
    ram       []byte
    key       uint32
    done      chan struct{}
}

func (lf *logFile) close() {
    fmt.Printf("close log, %d\n", lf.key)
    close(lf.done)
}

func (lf *logFile) register(buffer []byte) {
    lf.mxRAMOps.Lock()
    defer lf.mxRAMOps.Unlock()
    lf.ram = append(lf.ram, buffer...)

    atomic.AddUint32(&db.usesRam, uint32(len(buffer)))
}

func reallocate() []byte {
    return make([]byte, 0, rawFileSize)
}

func (lf *logFile) walk(f func(line []byte)) error {
    lf.mxFileOps.Lock()
    defer lf.mxFileOps.Unlock()
    scanner := bufio.NewScanner(lf.file)
    for scanner.Scan() {
        f(scanner.Bytes())
    }
    return scanner.Err()
}

func (lf *logFile) open() error {
    fmt.Printf("open log, %d\n", lf.key)
    var err error
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
    lf.openTime = time.Now()
    return nil
}

func (lf *logFile) margeFromRam() {
    if len(lf.ram) > 0 {
        lf.mxFileOps.Lock()
        {
            lf.mxRAMOps.Lock()
            {
                n, err := lf.file.Write(lf.ram)
                if err != nil {
                    lf.mxRAMOps.Unlock()
                    lf.mxFileOps.Unlock()
                    close(lf.done)
                    return
                }
                atomic.AddUint32(&db.usesRam, -uint32(n))
                lf.ram = reallocate()
            }
            lf.mxRAMOps.Unlock()
        }
        lf.mxFileOps.Unlock()
    }
}

func (lf *logFile) pusher() {
    doneTimer := make(chan struct{})
    timeTrigger := time.NewTicker(rateWriteSpeed)

    go func() {
        for {
            select {
            case <-timeTrigger.C:
                lf.margeFromRam()
            case <-doneTimer:
                return
            }
        }
    }()

    <-lf.done
    doneTimer <- struct{}{}
    if lf.file != nil {
        lf.file.Close()
    }
}
