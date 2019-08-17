package main

import (
    "bytes"
    "hash"
    "hash/adler32"
    "io"
    "os"
    "path/filepath"
    "sync"
    "syscall"
    "time"
)

// 50 Kb
const bufferSize = 50 << (10 * 1)

// 4 Mb
const rawFileSize = 4 << (10 * 2)
const rateWriteSpeed = time.Second

const flagDefault = os.O_CREATE | os.O_RDWR | os.O_APPEND
const permDefault = 0644

const maxOpenFilesDefault = 1000000

var bufferPool = sync.Pool{
    New: func() interface{} {
        return bytes.NewBuffer(make([]byte, 0, bufferSize))
    },
}

func releaseBuffer(buffer *bytes.Buffer) {
    buffer.Reset()
    bufferPool.Put(buffer)
}

var keyPool = sync.Pool{
    New: func() interface{} {
        return adler32.New()
    },
}

func releaseKey(key hash.Hash32) {
    key.Reset()
    keyPool.Put(key)
}

type database struct {
    basePath     string
    logFiles     map[uint32]*logFile
    maxOpenFiles int
    mx           sync.Mutex
}

func (d *database) registerBuffer(service, file string, buffer *bytes.Buffer) {
    h := keyPool.Get().(hash.Hash32)
    defer releaseKey(h)
    io.WriteString(h, service)
    io.WriteString(h, file)
    key := h.Sum32()

    var lf *logFile
    d.mx.Lock()
    lf, isInit := d.logFiles[key]
    if !isInit {
        if len(d.logFiles) > maxOpenFilesDefault {
            var lfLastOpen time.Time
            var keyToClose uint32
            for k, lf := range d.logFiles {
                if lf.file != nil && lfLastOpen.After(lf.open) {
                    keyToClose = k
                }
            }
            d.logFiles[keyToClose].close()
            delete(d.logFiles, keyToClose)
        }

        lf = &logFile{
            bufferBelt: make(chan []byte),
            ram:        reallocate(),
            key:        key,
            location:   filepath.Join(d.basePath, service, file+".db"),
        }

        // Запуск пушера
        go lf.pusher()

        d.logFiles[key] = lf
    }
    d.mx.Unlock()

    lf.register(buffer.Bytes())
}

type logFile struct {
    bufferBelt chan []byte
    mx         sync.RWMutex
    location   string
    open       time.Time
    file       *os.File
    ram        []byte
    key        uint32
}

func (lf *logFile) close() {
    if lf.file != nil {
        lf.file.Close()
    }
}

func (lf *logFile) register(buffer []byte) {
    lf.bufferBelt <- buffer
}

func reallocate() []byte {
    return make([]byte, 0, rawFileSize)
}

func (lf *logFile) walk(func()) {
    if len(lf.ram) > 0 {
        lf.mx.Lock()
        defer lf.mx.Unlock()
        if lf.file == nil {
            var err error
            lf.file, err = os.OpenFile(lf.location, flagDefault, permDefault)
            if e, ok := err.(*os.PathError); ok && e.Err == syscall.ERROR_PATH_NOT_FOUND {
                os.MkdirAll(filepath.Dir(lf.location), permDefault)
                lf.file, _ = os.OpenFile(lf.location, flagDefault, permDefault)
            }
            if lf.file == nil {
                return
            }
        }
        _, err := lf.file.Write(lf.ram)
        if err != nil {
            // emit stop register
            return
        }
        lf.ram = reallocate()
    }
}

func (lf *logFile) margeFromRam() {
    if len(lf.ram) > 0 {
        lf.mx.Lock()
        defer lf.mx.Unlock()
        if lf.file == nil {
            var err error
            lf.file, err = os.OpenFile(lf.location, flagDefault, permDefault)
            if e, ok := err.(*os.PathError); ok && e.Err == syscall.ERROR_PATH_NOT_FOUND {
                os.MkdirAll(filepath.Dir(lf.location), permDefault)
                lf.file, _ = os.OpenFile(lf.location, flagDefault, permDefault)
            }
            if lf.file == nil {
                return
            } else {
                lf.open = time.Now()
            }
        }
        _, err := lf.file.Write(lf.ram)
        if err != nil {
            // emit stop register
            return
        }
        lf.ram = reallocate()
    }
}

func (lf *logFile) pusher() {
    trigger := make(chan struct{})
    timeTrigger := time.NewTimer(rateWriteSpeed)
    go func() {
        for range timeTrigger.C {
            trigger <- struct{}{}
        }
    }()

    go func() {
        for range trigger {
            lf.margeFromRam()
        }
    }()

    for buffer := range lf.bufferBelt {
        lf.ram = append(lf.ram, buffer...)
        if len(lf.ram) > rawFileSize {
            trigger <- struct{}{}
        }
    }
}
