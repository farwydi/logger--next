package main

import (
    "github.com/gin-gonic/gin"
    "regexp"
)

func getLogsImpl(r *gin.Engine) {
    r.GET("logs/:service/:file", func(c *gin.Context) {
        lf, err := db.resolveLogFile(c.Param("service"), c.Param("file"))
        if err != nil {
            c.Status(404)
            return
        } else {
            c.Header("Content-Type", "application/json; charset=utf-8")
            c.Writer.WriteHeader(200)

            c.Writer.WriteString("[")
            first := true
            err = lf.walkByK(0, func(line []byte) {
                matched, _ := regexp.Match("Lincoln", line)
                if matched {
                    if first {
                        first = false
                    } else {
                        c.Writer.WriteString(",")
                    }
                    c.Writer.Write(line)
                }
            })
            c.Writer.WriteString("]")
            if err != nil {
                c.Error(err)
                c.Status(500)
                return
            }
        }
    })
}
