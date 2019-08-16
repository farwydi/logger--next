package main

import (
    "bufio"
    "bytes"
    "github.com/gin-gonic/gin"
    "io"
    "os"
    "regexp"
)

func handleLine(line string) []byte {
    matched, _ := regexp.MatchString("Lincoln", line)

    if matched {
        return []byte(line)
    }

    return nil
}

func main() {
    r := gin.Default()

    f, err := os.OpenFile("logfile.db", os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
    if err != nil {
        panic(err)
    }

    lf := &logFile{
        f: f,
    }

    r.PUT("/save_log", func(c *gin.Context) {
        lf.append(c.Request.Body)
        c.Status(200)
    })

    r.GET("logs", func(c *gin.Context) {
        var logsLine [][]byte

        err := lf.read(func(r io.Reader) error {
            scanner := bufio.NewScanner(r)

            for scanner.Scan() {
                line := handleLine(scanner.Text())
                if line != nil {
                    logsLine = append(logsLine, line)
                }
            }

            return scanner.Err()
        })

        if err != nil {
            c.JSON(500, c.Error(err).JSON())
            return
        }

        c.Header("Content-Type", "application/json; charset=utf-8")
        c.Writer.WriteHeader(200)
        c.Writer.Write([]byte("["))
        c.Writer.Write(bytes.Join(logsLine, []byte(",")))
        c.Writer.Write([]byte("]"))
    })

    r.Run(":4256")
}
