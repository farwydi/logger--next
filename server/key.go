package main

import (
    "hash"
    "hash/adler32"
    "io"
    "sync"
)

var keyPool = sync.Pool{
    New: func() interface{} {
        return adler32.New()
    },
}

func resolveKey(service, file string) uint32 {
    h := keyPool.Get().(hash.Hash32)
    defer releaseKey(h)
    io.WriteString(h, service)
    io.WriteString(h, file)
    return h.Sum32()
}

func releaseKey(key hash.Hash32) {
    key.Reset()
    keyPool.Put(key)
}
