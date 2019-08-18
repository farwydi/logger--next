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
}

func (h *headDb) find(key uint32) (found bool, err error) {
    h.mx.RLock()
    defer h.mx.RUnlock()

    file, err := os.Open(h.headFile)
    if err != nil {
        return false, err
    }
    defer file.Close()

    scanner := bufio.NewScanner(file)
    scanner.Buffer(make([]byte, 0, 600), bufio.MaxScanTokenSize)
    for scanner.Scan() {
        var r Row

        err = json.Unmarshal(scanner.Bytes(), &r)
        if err != nil {
            return false, err
        }

        if r.Key == key {
            found = true
            break
        }
    }

    return found, scanner.Err()
}

func (h *headDb) append(r Row) error {
    found, err := h.find(r.Key)
    if err != nil {
        return err
    }

    if found {
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
