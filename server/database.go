package main

import (
    "bytes"
    "path/filepath"
    "sync"
    "sync/atomic"
    "time"
)

const maxOpenFilesDefault = 1000000

// 4 Gb
const maxRamBuffer = 4 << 10 * 3

type database struct {
    usesRam  uint32
    basePath string
    logFiles map[uint32]*logFile
    mx       sync.Mutex
}

func (d *database) controlRam() {
    usesRam := atomic.LoadUint32(&d.usesRam)
    if usesRam > maxRamBuffer {
        d.closeOneOpenLogFile()
    }
}

func (d *database) closeOneOpenLogFile() {
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
        d.closeOneOpenLogFile()
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
    lf, err := d.resolveLogFile(service, file)
    if err != nil {
        return err
    }

    lf.register(buffer.Bytes())
    return nil
}
