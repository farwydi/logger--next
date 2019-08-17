package main

import (
    "bytes"
    "github.com/gin-gonic/gin"
    "hash"
    "io"
    "path/filepath"
    "regexp"
)


func handleLine(line string) []byte {
    matched, _ := regexp.MatchString("Lincoln", line)

    if matched {
        return []byte(line)
    }

    return nil
}

func getLogsImpl(r *gin.Engine)  {
    r.GET("logs/:service/:file", func(c *gin.Context) {
        var logsLine [][]byte

        h := keyPool.Get().(hash.Hash32)
        defer releaseKey(h)
        io.WriteString(h, c.Param("service"))
        io.WriteString(h, c.Param("file"))
        key := h.Sum32()

        db.mx.Lock()
        if lf, isInit := db.logFiles[key]; isInit {
            lf.walk()
        }
        db.mx.Unlock()

        //err := lf.read(func(r io.Reader) error {
        //    scanner := bufio.NewScanner(r)
        //
        //    for scanner.Scan() {
        //        line := handleLine(scanner.Text())
        //        if line != nil {
        //            logsLine = append(logsLine, line)
        //        }
        //    }
        //
        //    return scanner.Err()
        //})
        //
        //if err != nil {
        //    c.JSON(500, c.Error(err).JSON())
        //    return
        //}

        c.Header("Content-Type", "application/json; charset=utf-8")
        c.Writer.WriteHeader(200)
        c.Writer.Write([]byte("["))
        c.Writer.Write(bytes.Join(logsLine, []byte(",")))
        c.Writer.Write([]byte("]"))
    })
}
