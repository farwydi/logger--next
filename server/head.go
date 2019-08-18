package main

import (
    "bufio"
    "encoding/json"
    "os"
    "sync"
)

type Row struct {
    Service string `json:"service"`
    File    string `json:"file"`
    Key     uint32 `json:"key"`
    K       int    `json:"k"`
}

type headDb struct {
    headFile string
    mx       sync.RWMutex
    cacheMx  sync.RWMutex
    cache    []uint32
}

func (h *headDb) find(key uint32) bool {
    h.cacheMx.RLock()
    defer h.cacheMx.RUnlock()

    for _, v := range h.cache {
        if v == key {
            return true
        }
    }

    return false
}

func (h *headDb) append(r Row) error {
    if r.Key == 0 {
        r.Key = resolveKey(r.Service, r.File)
    }

    if h.find(r.Key) {
        return nil
    }

    h.mx.Lock()
    defer h.mx.Unlock()

    file, err := os.OpenFile(h.headFile, flagDefault, permDefault)
    if err != nil {
        return err
    }
    defer file.Close()

    line, err := json.Marshal(r)
    if err != nil {
        return err
    }

    _, err = file.Write(append(line, '\n'))
    if err != nil {
        return err
    }

    h.cacheMx.Lock()
    h.cache = append(h.cache, r.Key)
    h.cacheMx.Unlock()

    return nil
}

func (h *headDb) all() (rows []Row, err error) {
    h.mx.RLock()
    defer h.mx.RUnlock()

    file, err := os.Open(h.headFile)
    if err != nil {
        return nil, err
    }
    defer file.Close()

    scanner := bufio.NewScanner(file)
    scanner.Buffer(make([]byte, 0, 600), bufio.MaxScanTokenSize)
    for scanner.Scan() {
        var r Row

        err = json.Unmarshal(scanner.Bytes(), &r)
        if err != nil {
            return nil, err
        }

        rows = append(rows, r)
    }

    return rows, scanner.Err()
}
