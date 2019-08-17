package main

import (
    "bytes"
    "errors"
    "path/filepath"
    "sync"
    "time"
)

const maxOpenLogDuration = time.Second * 10

var (
    ErrShutdownNow = errors.New("shutdown now")
)

type database struct {
    usesRam     uint32
    basePath    string
    logFiles    map[uint32]*logFile
    done        chan struct{}
    mx          sync.RWMutex
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
        timeGCRam := time.NewTicker(maxOpenLogDuration)

        go func() {
            for {
                select {
                case <-timeGCRam.C:
                    db.closeLongOpenLogFile()
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
    }()

    return db
}

func (d *database) closeLongOpenLogFile() {
    db.mx.Lock()
    defer db.mx.Unlock()

    for k, lf := range d.logFiles {
        if lf.file != nil && time.Since(lf.lastOps) > maxOpenLogDuration {
            lf.close()
            delete(d.logFiles, k)
        }
    }
}

func (d *database) initLogFile(service, file string, key uint32) (lf *logFile, err error) {
    db.mx.Lock()
    defer db.mx.Unlock()

    lf = &logFile{
        key:      key,
        location: filepath.Join(d.basePath, service, file+".db"),
    }
    d.logFiles[key] = lf

    return lf, nil
}

func (d *database) resolveLogFile(service, file string) (lf *logFile, err error) {
    key := resolveKey(service, file)

    d.mx.RLock()
    lf, found := d.logFiles[key]
    d.mx.RUnlock()

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

    return lf.register(buffer.Bytes())
}
