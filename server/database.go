package main

import (
    "bytes"
    "errors"
    "fmt"
    "path/filepath"
    "sync"
    "sync/atomic"
    "time"
)

const maxOpenFilesDefault = 1000000
const rateGCRam = time.Second

// 4 Gb
const maxRamBuffer = 4 << 10 * 3

var (
    ErrShutdownNow = errors.New("shutdown now")
)

type database struct {
    usesRam     uint32
    basePath    string
    logFiles    map[uint32]*logFile
    done        chan struct{}
    mx          sync.Mutex
    shutdownNow bool
}

func (d *database) shutdown() {
    d.shutdownNow = true
    close(d.done)
}

func newDatabase() *database {
    var db = &database{
        logFiles: make(map[uint32]*logFile),
        basePath: "logs",
        done:     make(chan struct{}),
    }

    go func() {
        doneTimer := make(chan struct{})
        timeGCRam := time.NewTicker(rateGCRam)

        go func() {
            for {
                select {
                case <-timeGCRam.C:
                    usesRam := atomic.LoadUint32(&db.usesRam)
                    fmt.Printf("ram: %d\n", usesRam)
                    for usesRam > maxRamBuffer {
                        db.closeOneOldOpenLogFile()
                        usesRam = atomic.LoadUint32(&db.usesRam)
                    }
                case <-doneTimer:
                    return
                }
            }
        }()

        <-db.done
        doneTimer <- struct{}{}

        for _, lf := range db.logFiles {
            lf.close()
        }
        db.logFiles = nil
        fmt.Printf("database done\n")
    }()

    return db
}

func (d *database) closeOneOldOpenLogFile() {
    db.mx.Lock()
    defer db.mx.Unlock()

    var lfLastOpen time.Time
    var keyToClose uint32
    for k, lf := range d.logFiles {
        if lf.file != nil && lfLastOpen.After(lf.openTime) {
            keyToClose = k
        }
    }
    d.logFiles[keyToClose].close()
    delete(d.logFiles, keyToClose)
}

func (d *database) initLogFile(service, file string, key uint32) (lf *logFile, err error) {
    db.mx.Lock()
    defer db.mx.Unlock()

    if len(d.logFiles) > maxOpenFilesDefault {
        d.closeOneOldOpenLogFile()
    }

    lf = &logFile{
        done:     make(chan struct{}),
        ram:      reallocate(),
        key:      key,
        location: filepath.Join(d.basePath, service, file+".db"),
    }
    if err := lf.open(); err == nil {
        // Запуск пушера
        go lf.pusher()

        d.logFiles[key] = lf
    } else {
        return nil, err
    }

    return lf, nil
}

func (d *database) resolveLogFile(service, file string) (lf *logFile, err error) {
    key := resolveKey(service, file)

    d.mx.Lock()
    lf, found := d.logFiles[key]
    d.mx.Unlock()
    if !found {
        lf, err = d.initLogFile(service, file, key)
        if err != nil {
            return nil, err
        }
    }

    return lf, nil
}

func (d *database) registerBuffer(service, file string, buffer *bytes.Buffer) error {
    if d.shutdownNow {
        return ErrShutdownNow
    }

    lf, err := d.resolveLogFile(service, file)
    if err != nil {
        return err
    }

    lf.register(buffer.Bytes())
    return nil
}
