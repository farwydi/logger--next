package main

import (
    "github.com/Pallinder/go-randomdata"
    "go.uber.org/zap"
    "go.uber.org/zap/zapcore"
    "math/rand"
    "testing"
)

// Results
// BenchmarkSpeedWrite-12       1000000	        1965 ns/op Максимальная теоретическая скорость
// BenchmarkSpeedWrite-4        2000	        985671 ns/op = not optimz
// BenchmarkSpeedWrite-4        10000	        155699 ns/op = keep-alive 15
// BenchmarkSpeedWrite-4        10000	        153599 ns/op = keep-alive 100
// BenchmarkSpeedWrite-12    	10000   	    194700 ns/op
// BenchmarkSpeedWrite-12    	1000000	        2065 ns/op
// BenchmarkSpeedWrite-12    	500000	        3571 ns/op
func BenchmarkSpeedWrite(b *testing.B) {
    logger := zap.New(
        zapcore.NewCore(
            zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
            NewWriter("http://localhost:4256/save_log/test/main", "localhost:4256", 0),
            zap.NewAtomicLevelAt(zap.InfoLevel),
        ),
    )

    for i := 0; i < b.N; i++ {
        logger.Info(randomdata.Address(), zap.Int("int", rand.Int()))
    }
    logger.Sync()
}

// BenchmarkSpeedWriteParallel-12    	   10000	    191200 ns/op
// BenchmarkSpeedWriteParallel-12    	 1000000	      1446 ns/op
// BenchmarkSpeedWriteParallel-12    	  500000	      2792 ns/op
func BenchmarkSpeedWriteParallel(b *testing.B) {
    logger := zap.New(
        zapcore.NewCore(
            zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
            NewWriter("http://localhost:4256/save_log/test/main", "localhost:4256", 0),
            zap.NewAtomicLevelAt(zap.InfoLevel),
        ),
    )

    b.RunParallel(func(pb *testing.PB) {
        for pb.Next() {
            logger.Info(randomdata.Address(), zap.Int("int", rand.Int()))
        }
        logger.Sync()
    })
}
