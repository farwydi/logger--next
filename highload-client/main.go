package main

import (
    "fmt"
    "github.com/Pallinder/go-randomdata"
    "github.com/valyala/fasthttp"
    "go.uber.org/zap"
    "go.uber.org/zap/zapcore"
    "math/rand"
    "os"
    "sync"
    "time"
)

// 50 Kb
const bufferSize = 50 << (10 * 1)
const ct = "application/json; charset=utf-8"

func NewWriter(url, addr string, rate time.Duration) *NetworkWriter {
    fw := &NetworkWriter{
        c: &fasthttp.HostClient{
            Addr:                     addr,
            MaxConns:                 10,
            MaxIdleConnDuration:      time.Second * 10,
            NoDefaultUserAgentHeader: true,
        },
        url:    url,
        buffer: reallocate(),
    }

    if rate != 0 {
        fw.pusher = time.NewTicker(rate)
        fw.done = make(chan struct{})
        go func() {
            for {
                select {
                case <-fw.pusher.C:
                    err := fw.Sync()
                    if err != nil {
                        fmt.Fprintf(os.Stderr, "%v NetworkWriter.Sync error: %v\n", time.Now(), err)
                    }
                case <-fw.done:
                    return
                }
            }
        }()
    }

    return fw
}

func reallocate() []byte {
    return make([]byte, 0, bufferSize)
}

type NetworkWriter struct {
    c      *fasthttp.HostClient
    url    string
    buffer []byte
    mx     sync.Mutex
    pusher *time.Ticker
    done   chan struct{}
}

func (fw *NetworkWriter) Stop() {
    if fw.pusher != nil {
        fw.pusher.Stop()
        fw.done <- struct{}{}
        close(fw.done)
    }

    err := fw.Sync()
    if err != nil {
        fmt.Fprintf(os.Stderr, "%v NetworkWriter.Stop error: failed Sync, %v\n", time.Now(), err)
        os.Stderr.Write(fw.buffer)
        fmt.Fprintf(os.Stderr, "%v NetworkWriter.Stop done buffer dump\n", time.Now())
    }
}

func (fw *NetworkWriter) Write(p []byte) (n int, err error) {
    fw.mx.Lock()
    defer fw.mx.Unlock()
    fw.buffer = append(fw.buffer, p...)
    return len(p), nil
}

func (fw *NetworkWriter) Sync() error {
    fw.mx.Lock()
    defer fw.mx.Unlock()

    if !(len(fw.buffer) > 0) {
        return nil
    }

    req := fasthttp.AcquireRequest()
    defer fasthttp.ReleaseRequest(req)
    req.SetRequestURI(fw.url)
    req.Header.SetMethod(fasthttp.MethodPut)
    req.Header.Set(fasthttp.HeaderConnection, "keep-alive")

    resp := fasthttp.AcquireResponse()
    defer fasthttp.ReleaseResponse(resp)

    //fasthttp.WriteGzipLevel(req.BodyWriter(), fw.buffer, gzip.BestSpeed)
    req.SetBody(fw.buffer)
    err := fw.c.DoTimeout(req, resp, time.Millisecond*340)
    if err != nil {
        return err
    }

    statusCode := resp.Header.StatusCode()
    if statusCode != fasthttp.StatusOK {
        return fmt.Errorf(
            "unexpected status code: %d. Expecting %d", statusCode, fasthttp.StatusOK,
        )
    }

    fw.buffer = reallocate()
    return nil
}

func main() {
    fw := NewWriter("http://localhost:4256/save_log/test/mainxx", "localhost:4256", time.Second)
    defer fw.Stop()

    logger := zap.New(
        zapcore.NewCore(
            zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
            fw,
            zap.NewAtomicLevelAt(zap.InfoLevel),
        ),
    )

    logger.Info("New message", zap.Int("int", 15))
    for i := 0; i < 1572864; i++ {
        logger.Info(randomdata.Address(), zap.Int("int", rand.Int()))
        time.Sleep(time.Nanosecond)
    }
}
