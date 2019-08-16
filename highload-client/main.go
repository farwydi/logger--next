package main

import (
    "fmt"
    "github.com/Pallinder/go-randomdata"
    "github.com/valyala/fasthttp"
    "go.uber.org/zap"
    "go.uber.org/zap/zapcore"
    "math/rand"
    "time"
)

func NewWriter(url string) *FastWrite {
    req := &fasthttp.Request{}
    req.SetRequestURI("http://localhost:4256/save_log")
    req.Header.SetMethod(fasthttp.MethodPut)

    return &FastWrite{
        c: &fasthttp.HostClient{
            Addr:     "localhost:4256",
            MaxConns: 20,
        },
        req:    req,
        buffer: make([]byte, 50000),
    }
}

func RegisterSync(fw *FastWrite) {
    go func() {
        fw.Sync()
    }()
}

type FastWrite struct {
    c      *fasthttp.HostClient
    req    *fasthttp.Request
    buffer []byte
}

func (fw *FastWrite) max() int {
    return 50000
}

func (fw *FastWrite) Write(p []byte) (n int, err error) {
    if len(fw.buffer) + len(p) > fw.max() {
        fw.buffer = p
        return 0, nil
    }

    fw.buffer = append(fw.buffer, p...)
    return len(p), nil
}

func (fw *FastWrite) Sync() error {
    resp := fasthttp.AcquireResponse()
    defer fasthttp.ReleaseResponse(resp)
    fw.req.SetBody(fw.buffer)
    err := fw.c.DoTimeout(fw.req, resp, time.Millisecond*120)
    if err != nil {
        return err
    }

    statusCode := resp.Header.StatusCode()
    if statusCode != fasthttp.StatusOK {
        return fmt.Errorf(
            "unexpected status code: %d. Expecting %d", statusCode, fasthttp.StatusOK,
        )
    }

    return nil
}

func main() {
    logger := zap.New(
        zapcore.NewCore(
            zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
            NewWriter("http://localhost:4256/save_log"),
            zap.NewAtomicLevelAt(zap.InfoLevel),
        ),
    )

    logger.Info("New message", zap.Int("int", 15))

    for i := 0; i < 100000; i++ {
        logger.Info(randomdata.Address(), zap.Int("int", rand.Int()))
    }
}
