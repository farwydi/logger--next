package main

import (
    "bytes"
    "sync"
)

// 50 Kb
const bufferSize = 50 << (10 * 1)

var bufferPool = sync.Pool{
    New: func() interface{} {
        return bytes.NewBuffer(make([]byte, 0, bufferSize))
    },
}

func releaseBuffer(buffer *bytes.Buffer) {
    buffer.Reset()
    bufferPool.Put(buffer)
}
