package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/soup/jobqueue/internal/api"
	"github.com/soup/jobqueue/internal/store"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	pool, err := store.Connect(ctx, env("DATABASE_URL", "postgres://soup:soup@localhost:5432/soup"), 32)
	if err != nil {
		log.Error("connect", "err", err)
		os.Exit(1)
	}
	defer pool.Close()
	st := store.New(pool)

	// Postgres may still be starting (docker compose); retry migrations for a while.
	for i := 0; ; i++ {
		if err = st.Migrate(ctx); err == nil {
			break
		}
		if i == 30 {
			log.Error("migrate", "err", err)
			os.Exit(1)
		}
		time.Sleep(time.Second)
	}

	srv := &http.Server{
		Addr:              env("ADDR", ":8080"),
		Handler:           (&api.Server{Store: st, Log: log}).Routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shCtx, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		_ = srv.Shutdown(shCtx)
	}()
	log.Info("api listening", "addr", srv.Addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("serve", "err", err)
		os.Exit(1)
	}
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
