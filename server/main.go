package main

import (
    "bytes"
    "compress/gzip"
    "fmt"
    "github.com/gin-gonic/gin"
    "golang.org/x/net/context"
    "net/http"
    "os"
    "os/signal"
    "sync"
    "syscall"
    "time"
)

var db = &database{
    logFiles: make(map[uint32]*logFile),
    basePath: "logs",
}

func main() {
    gin.SetMode(gin.ReleaseMode)
    r := gin.Default()

    var gzPool sync.Pool
    gzPool.New = func() interface{} {
        return new(gzip.Reader)
    }

    r.PUT("/save_log/:service/:file", func(c *gin.Context) {
        buffer := bufferPool.Get().(*bytes.Buffer)
        defer releaseBuffer(buffer)

        _, err := buffer.ReadFrom(c.Request.Body)
        if err != nil {
            c.Status(500)
            return
        }

        db.registerBuffer(c.Param("service"), c.Param("file"), buffer)
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
        if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
            fmt.Fprintf(os.Stderr, "Listen: %s\n", err)
        }
    }()

    quit := make(chan os.Signal)
    signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

    <-quit
    fmt.Println("Shutdown server ...")

    ctx, cancel := context.WithTimeout(context.Background(), time.Second)
    defer cancel()
    if err := srv.Shutdown(ctx); err != nil {
        fmt.Fprintf(os.Stderr, "Server shutdown: %s", err)
    }

    select {
    case <-ctx.Done():
        fmt.Println("Timeout shutdown")
    }
    fmt.Println("Server exiting")
}
