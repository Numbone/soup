package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/soup/jobqueue/internal/store"
	"github.com/soup/jobqueue/internal/worker"
)

func main() {
	var cfg worker.Config
	flag.DurationVar(&cfg.Lease, "lease", 3*time.Second, "lease duration; heartbeat every lease/3")
	flag.DurationVar(&cfg.PollInterval, "poll", 100*time.Millisecond, "idle poll interval")
	flag.DurationVar(&cfg.ReapInterval, "reap", time.Second, "how often this worker reaps expired leases")
	flag.BoolVar(&cfg.UnsafeNoFence, "unsafe-no-fence", false, "NEGATIVE CONTROL ONLY: complete without fencing")
	flag.BoolVar(&cfg.UnsafeNoHeartbeat, "unsafe-no-heartbeat", false, "NEGATIVE CONTROL ONLY: never extend the lease")
	flag.Parse()

	host, _ := os.Hostname()
	b := make([]byte, 3)
	_, _ = rand.Read(b)
	cfg.ID = fmt.Sprintf("%s-%d-%s", host, os.Getpid(), hex.EncodeToString(b))
	logger := log.New(os.Stdout, "worker="+cfg.ID+" ", log.Lmicroseconds)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	url := os.Getenv("DATABASE_URL")
	if url == "" {
		url = "postgres://soup:soup@localhost:5432/soup"
	}
	pool, err := store.Connect(ctx, url, 4)
	if err != nil {
		logger.Fatalf("ERROR connect: %v", err)
	}
	defer pool.Close()

	logger.Printf("START lease=%s unsafe_no_fence=%v", cfg.Lease, cfg.UnsafeNoFence)
	w := &worker.Worker{Store: store.New(pool), Cfg: cfg, Log: logger}
	_ = w.Run(ctx)
	logger.Printf("STOP")
}
