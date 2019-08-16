package main

import (
    "fmt"
    logger2 "git.immo/repositories/b3w-grpc-go-lib.git/logger"
    "github.com/Pallinder/go-randomdata"
    "github.com/google/uuid"
    "go.uber.org/zap"
    "go.uber.org/zap/zapcore"
    "math/rand"
    "os"
    "testing"
)

// Results
// BenchmarkSpeedWrite-4   	    2000	    985671 ns/op = not optimz
// BenchmarkSpeedWrite-4   	   10000	    155699 ns/op = keep-alive 15
// BenchmarkSpeedWrite-4   	   10000	    153599 ns/op = keep-alive 100
func BenchmarkSpeedWrite(b *testing.B) {
    logger := zap.New(
        zapcore.NewCore(
            zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
            NewWriter("http://localhost:4256/save_log"),
            zap.NewAtomicLevelAt(zap.InfoLevel),
        ),
    )

    for i := 0; i < b.N; i++ {
        logger.Info(randomdata.Address(), zap.Int("int", rand.Int()))
    }
}

func BenchmarkSpeedWriteA(b *testing.B) {
    session := uuid.New().String()
    os.Unsetenv("HTTP_PROXY")
    os.Unsetenv("HTTPS_PROXY")

    logger2.GrpcLoggerServiceConnection = "vps2085.mtu.immo:9516"

    for i := 0; i < b.N; i++ {
        _, err := logger2.Add(logger2.LogMessage{
            Alias:        "benchmarkSpeedWriteA",
            MessageText:  randomdata.Address(),
            MessageType:  "INFO",
            MessageTrace: fmt.Sprintf("int=%d", rand.Int()),
            Session:      session,
        })
        if err != nil {
            b.Fatal(err)
        }
    }
}

func BenchmarkSpeedWriteB(b *testing.B) {
    os.Unsetenv("HTTP_PROXY")
    os.Unsetenv("HTTPS_PROXY")

    logger2.GrpcLoggerServiceConnection = "vps2085.mtu.immo:9516"

    b.RunParallel(func(pb *testing.PB) {
        session := uuid.New().String()
        for pb.Next() {
            _, err := logger2.Add(logger2.LogMessage{
                Alias:        "BenchmarkSpeedWriteB",
                MessageText:  randomdata.Address(),
                MessageType:  "INFO",
                MessageTrace: fmt.Sprintf("int=%d", rand.Int()),
                Session:      session,
            })
            if err != nil {
                b.Fatal(err)
            }
        }
    })
}
