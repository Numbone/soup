// Package api is the HTTP layer. It contains no state logic: every decision is in store.
package api

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/soup/jobqueue/internal/store"
)

type Server struct {
	Store *store.Store
	Log   *slog.Logger
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /jobs", s.create)
	mux.HandleFunc("GET /jobs", s.list)
	mux.HandleFunc("GET /jobs/{id}", s.get)
	mux.HandleFunc("POST /jobs/{id}/cancel", s.cancel)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := s.Store.Pool.Ping(r.Context()); err != nil {
			writeErr(w, http.StatusServiceUnavailable, err.Error())
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	return mux
}

type createReq struct {
	IdempotencyKey string          `json:"idempotency_key"`
	Payload        json.RawMessage `json:"payload"`
}

// POST /jobs. Key comes from the Idempotency-Key header or the body field.
// 201 = created, 200 = replay of the same request, 409 = same key with a different payload.
func (s *Server) create(w http.ResponseWriter, r *http.Request) {
	var req createReq
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeErr(w, http.StatusRequestEntityTooLarge, "body larger than 1MiB")
			return
		}
		writeErr(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if h := r.Header.Get("Idempotency-Key"); h != "" {
		if req.IdempotencyKey != "" && req.IdempotencyKey != h {
			writeErr(w, http.StatusBadRequest, "Idempotency-Key header and body idempotency_key differ")
			return
		}
		req.IdempotencyKey = h
	}
	if req.IdempotencyKey == "" || len(req.IdempotencyKey) > 200 {
		writeErr(w, http.StatusBadRequest, "idempotency key required (1..200 chars)")
		return
	}
	if len(req.Payload) == 0 {
		req.Payload = json.RawMessage(`{}`)
	}
	job, created, err := s.Store.Create(r.Context(), req.IdempotencyKey, req.Payload)
	switch {
	case errors.Is(err, store.ErrIdempotencyConflict):
		writeErr(w, http.StatusConflict, err.Error())
	case err != nil && strings.HasPrefix(err.Error(), "payload is not valid JSON"):
		writeErr(w, http.StatusBadRequest, err.Error())
	case err != nil:
		s.internal(w, r, "create", err)
	case created:
		writeJSON(w, http.StatusCreated, job)
	default:
		writeJSON(w, http.StatusOK, job)
	}
}

func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	job, err := s.Store.Get(r.Context(), id)
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeErr(w, http.StatusNotFound, err.Error())
	case err != nil:
		s.internal(w, r, "get", err)
	default:
		writeJSON(w, http.StatusOK, job)
	}
}

// GET /jobs?state=queued&limit=50&cursor=<id>. next_cursor is the last id of the page; absent on the last page.
func (s *Server) list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit := 50
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 500 {
			writeErr(w, http.StatusBadRequest, "limit must be 1..500")
			return
		}
		limit = n
	}
	var cursor int64
	if v := q.Get("cursor"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 0 {
			writeErr(w, http.StatusBadRequest, "invalid cursor")
			return
		}
		cursor = n
	}
	state := q.Get("state")
	switch state {
	case "", store.StateQueued, store.StateRunning, store.StateSucceeded, store.StateFailed, store.StateCancelled:
	default:
		writeErr(w, http.StatusBadRequest, "unknown state")
		return
	}
	jobs, err := s.Store.List(r.Context(), state, cursor, limit+1)
	if err != nil {
		s.internal(w, r, "list", err)
		return
	}
	resp := map[string]any{"jobs": jobs}
	if len(jobs) > limit {
		resp["jobs"] = jobs[:limit]
		resp["next_cursor"] = strconv.FormatInt(jobs[limit-1].ID, 10)
	}
	writeJSON(w, http.StatusOK, resp)
}

// POST /jobs/{id}/cancel. 200 = cancelled (also when it already was), 409 = already succeeded/failed.
func (s *Server) cancel(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	job, err := s.Store.Cancel(r.Context(), id)
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeErr(w, http.StatusNotFound, err.Error())
	case errors.Is(err, store.ErrNotCancellable):
		writeJSON(w, http.StatusConflict, map[string]any{"error": err.Error(), "job": job})
	case err != nil:
		s.internal(w, r, "cancel", err)
	default:
		writeJSON(w, http.StatusOK, job)
	}
}

func pathID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		writeErr(w, http.StatusBadRequest, "invalid id")
		return 0, false
	}
	return id, true
}

func (s *Server) internal(w http.ResponseWriter, r *http.Request, op string, err error) {
	if r.Context().Err() != nil {
		// client went away (e.g. its own timeout). Not a server error; the tx outcome is whatever
		// committed, and a retry with the same idempotency key returns it.
		s.Log.Info("client gone", "op", op, "err", err)
		writeErr(w, 499, "client closed request")
		return
	}
	if dbUnavailable(err) {
		// The request may or may not have committed; retrying with the same idempotency key is safe.
		s.Log.Warn("db unavailable", "op", op, "err", err)
		w.Header().Set("Retry-After", "1")
		writeErr(w, http.StatusServiceUnavailable, "database unavailable, retry")
		return
	}
	s.Log.Error("request failed", "op", op, "err", err)
	writeErr(w, http.StatusInternalServerError, "internal error")
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// dbUnavailable: connection refused/reset, server shutting down (57P0x), or EOF mid-query.
func dbUnavailable(err error) bool {
	var connErr *pgconn.ConnectError
	if errors.As(err, &connErr) || errors.Is(err, io.ErrUnexpectedEOF) || pgconn.Timeout(err) {
		return true
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && strings.HasPrefix(pgErr.Code, "57P") {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr)
}
