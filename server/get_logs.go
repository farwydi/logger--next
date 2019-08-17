package main

import (
    "bytes"
    "fmt"
    "github.com/gin-gonic/gin"
    "os"
    "regexp"
)

func handleLine(line []byte) []byte {
    matched, _ := regexp.Match("Lincoln", line)

    if matched {
        return line
    }

    return nil
}

func getLogsImpl(r *gin.Engine) {
    r.GET("logs/:service/:file", func(c *gin.Context) {
        var logsLine [][]byte

        lf, err := db.resolveLogFile(c.Param("service"), c.Param("file"))
        if err != nil {
            c.Status(404)
            return
        } else {
            err = lf.walk(func(line []byte) {
                if addLine := handleLine(line); addLine != nil {
                    logsLine = append(logsLine, line)
                }
            })
            if err != nil {
                fmt.Fprintf(os.Stderr, "%v\n", err)
                c.Status(500)
                return
            }
        }

        c.Header("Content-Type", "application/json; charset=utf-8")
        c.Writer.WriteHeader(200)
        c.Writer.Write([]byte("["))
        c.Writer.Write(bytes.Join(logsLine, []byte(",")))
        c.Writer.Write([]byte("]"))
    })
}
