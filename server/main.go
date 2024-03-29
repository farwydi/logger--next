package main

import (
    "bytes"
    "fmt"
    "github.com/gin-gonic/gin"
    "golang.org/x/net/context"
    "net/http"
    "os"
    "os/signal"
    "syscall"
    "time"
)

var db *database

func init()  {
    var err error

    db, err = newDatabase()
    if err != nil {
        panic(err)
    }
}

func main() {
    gin.SetMode(gin.ReleaseMode)
    r := gin.Default()

    r.GET("/head", func(c *gin.Context) {
        rows, err := db.head.all()
        if err != nil {
            c.Status(500)
            return
        }

        if rows == nil {
            rows = make([]Row, 0)
        }

        c.JSON(200, rows)
    })

    r.PUT("/save_log/:service/:file", func(c *gin.Context) {
        buffer := bufferPool.Get().(*bytes.Buffer)
        defer releaseBuffer(buffer)

        _, err := buffer.ReadFrom(c.Request.Body)
        if err != nil {
            c.Status(500)
            return
        }

        err = db.registerBuffer(c.Param("service"), c.Param("file"), buffer)
        if err != nil {
            c.Status(500)
            return
        }

        c.Status(200)
    })

    getLogsImpl(r)

    srv := &http.Server{
        Addr:         ":4256",
        Handler:      r,
        ReadTimeout:  time.Second,
        WriteTimeout: 500 * time.Millisecond,
        IdleTimeout:  12 * time.Second,
    }

    go func() {
        // service connections
        fmt.Printf("%v Start http server endpoint\n", time.Now())
        if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
            fmt.Fprintf(os.Stderr, "%v Listen: %s\n", time.Now(), err)
        }
    }()

    quit := make(chan os.Signal)
    signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

    <-quit
    fmt.Printf("%v Shutdown server ...\n", time.Now())
    srv.SetKeepAlivesEnabled(false)
    db.shutdown()

    ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
    defer cancel()
    if err := srv.Shutdown(ctx); err != nil {
        fmt.Fprintf(os.Stderr, "%v Could not gracefully shutdown the server: %s", time.Now(), err)
    }
    fmt.Printf("%v Server exiting\n", time.Now())
}
