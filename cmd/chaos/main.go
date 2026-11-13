// Command chaos is the fault-injection harness.
//
// It owns the whole system under test: it resets the database, starts one API process and N worker
// processes as children, then concurrently
//   - submits jobs over HTTP (some as 3 simultaneous identical POSTs, some as "timed out then retried"),
//   - replays already-created keys (must return the same job) and reuses keys with a different
//     payload (must return 409),
//   - cancels a fraction of jobs at random moments (some twice),
//   - polls job status over HTTP to check that a terminal state never changes,
//   - kill -9's a random worker about every 2s (and respawns it so N stay alive),
//   - SIGSTOPs a random worker for longer than the lease and then SIGCONTs it (a real zombie),
//
// waits until no job is queued/running, kills everything, and checks invariants straight in the DB.
//
// Exit code 0 = every invariant held, 1 = at least one violation, 2 = harness error.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type config struct {
	dbURL, apiBin, workerBin, workerArgs, logDir, label  string
	jobs, workers, producers                             int
	rate                                                 float64
	killEvery, stallEvery, stallFor, lease, timeout      time.Duration
	minMS, maxMS                                         int
	cancelFrac, failFrac, burstFrac, retryFrac, longFrac float64
	dupFrac, conflictFrac, doubleCancelFrac              float64
	seed                                                 uint64
}

func main() {
	var c config
	flag.StringVar(&c.dbURL, "db", envOr("DATABASE_URL", "postgres://soup:soup@localhost:5432/soup_chaos"), "postgres URL (database is WIPED)")
	flag.StringVar(&c.apiBin, "api-bin", "./bin/api", "path to api binary")
	flag.StringVar(&c.workerBin, "worker-bin", "./bin/worker", "path to worker binary")
	flag.StringVar(&c.workerArgs, "worker-args", "", "extra args for every worker, e.g. -unsafe-no-fence")
	flag.StringVar(&c.logDir, "logdir", "runs/logs", "where child process logs go")
	flag.StringVar(&c.label, "label", "default", "run label (for the report)")
	flag.IntVar(&c.jobs, "jobs", 10000, "distinct jobs to create")
	flag.IntVar(&c.workers, "workers", 8, "worker processes kept alive")
	flag.IntVar(&c.producers, "producers", 16, "concurrent HTTP submitters")
	flag.Float64Var(&c.rate, "rate", 150, "job submissions per second (0 = unlimited)")
	flag.DurationVar(&c.killEvery, "kill-every", 2*time.Second, "mean interval between kill -9 (uniform +/-50%)")
	flag.DurationVar(&c.stallEvery, "stall-every", 7*time.Second, "mean interval between SIGSTOP zombies (0 = off)")
	flag.DurationVar(&c.stallFor, "stall-for", 6*time.Second, "how long a zombie stays frozen (> lease to force reclaim)")
	flag.DurationVar(&c.lease, "lease", 3*time.Second, "worker lease")
	flag.DurationVar(&c.timeout, "timeout", 20*time.Minute, "give up waiting for drain after this")
	flag.IntVar(&c.minMS, "min-ms", 10, "min emulated job duration")
	flag.IntVar(&c.maxMS, "max-ms", 200, "max emulated job duration")
	flag.Float64Var(&c.cancelFrac, "cancel-frac", 0.10, "fraction of jobs that get a cancel request")
	flag.Float64Var(&c.doubleCancelFrac, "double-cancel-frac", 0.2, "fraction of cancels that are sent twice")
	flag.Float64Var(&c.longFrac, "long-frac", 0.01, "fraction of jobs lasting 2-3x the lease (exercises heartbeats and cancel-while-running)")
	flag.Float64Var(&c.failFrac, "fail-frac", 0.02, "fraction of jobs whose payload fails deterministically")
	flag.Float64Var(&c.burstFrac, "burst-frac", 0.10, "fraction of jobs submitted as 3 simultaneous identical POSTs")
	flag.Float64Var(&c.retryFrac, "retry-frac", 0.05, "fraction submitted with a ~1ms client timeout, then retried")
	flag.Float64Var(&c.dupFrac, "dup-frac", 0.30, "fraction of jobs re-POSTed later with the same key+payload")
	flag.Float64Var(&c.conflictFrac, "conflict-frac", 0.02, "fraction of keys re-POSTed with a DIFFERENT payload")
	flag.Uint64Var(&c.seed, "seed", uint64(time.Now().UnixNano()), "random seed")
	flag.Parse()

	h := &harness{cfg: c, rng: rand.New(rand.NewPCG(c.seed, c.seed^0x9e3779b97f4a7c15))}
	os.Exit(h.run())
}

// ---------------------------------------------------------------- harness state

type harness struct {
	cfg   config
	rngMu sync.Mutex
	rng   *rand.Rand
	pool  *pgxpool.Pool
	base  string
	http  *http.Client
	start time.Time

	logMu   sync.Mutex
	logFile *os.File

	procMu  sync.Mutex
	workers []*proc
	api     *proc
	stopAll atomic.Bool

	// chaos + worker log counters
	kills, stalls, spawns                         atomic.Int64
	claims, dones, fenced, leaseLost, failedLines atomic.Int64
	reapRequeued, reapFailed, workerErrs, apiErrs atomic.Int64
	errSamples                                    []string
	doneJobs                                      []int64 // (job, attempt) pairs a worker reported DONE
	doneAttempts                                  []int32

	// client-side ground truth
	mu           sync.Mutex
	plan         []jobPlan
	idByIdx      []int64 // 0 = never got an id
	got201       []int   // how many 201 Created responses each key received (must be <= 1)
	created      []int   // indices with a known id (for random picks)
	cancelledOK  map[int64]bool
	observed     map[int64]obs
	violations   []string
	http5xx      atomic.Int64
	httpNetErrs  atomic.Int64
	posts201     atomic.Int64
	posts200     atomic.Int64
	dupReplays   atomic.Int64
	conflictsOK  atomic.Int64
	cancelReqs   atomic.Int64
	cancel200    atomic.Int64
	cancel409    atomic.Int64
	timeoutRetry atomic.Int64
	bursts       atomic.Int64
	observations atomic.Int64
	pendingAsync sync.WaitGroup
}

type jobPlan struct {
	key     string
	payload []byte
	fail    bool
}

type obs struct {
	terminal string
	attempt  int
}

type proc struct {
	name    string
	cmd     *exec.Cmd
	dead    atomic.Bool
	stopped atomic.Bool
	done    chan struct{}
}

func (h *harness) randN(n int) int {
	h.rngMu.Lock()
	defer h.rngMu.Unlock()
	return h.rng.IntN(n)
}

func (h *harness) chance(p float64) bool {
	h.rngMu.Lock()
	defer h.rngMu.Unlock()
	return h.rng.Float64() < p
}

func (h *harness) jitter(d time.Duration) time.Duration { // uniform in [0.5d, 1.5d]
	h.rngMu.Lock()
	defer h.rngMu.Unlock()
	return d/2 + time.Duration(h.rng.Int64N(int64(d)+1))
}

func (h *harness) violate(format string, a ...any) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.violations) < 10000 {
		h.violations = append(h.violations, fmt.Sprintf(format, a...))
	}
}

func (h *harness) logf(format string, a ...any) {
	fmt.Printf("[%7.2fs] %s\n", time.Since(h.start).Seconds(), fmt.Sprintf(format, a...))
}

// ---------------------------------------------------------------- main flow

func (h *harness) run() int {
	c := h.cfg
	h.start = time.Now()
	fmt.Printf("=== CHAOS RUN label=%s seed=%d started=%s\n", c.label, c.seed, h.start.UTC().Format(time.RFC3339))
	fmt.Printf("config: jobs=%d workers=%d rate=%.0f/s kill_every=%s stall_every=%s stall_for=%s lease=%s dur=%d..%dms\n",
		c.jobs, c.workers, c.rate, c.killEvery, c.stallEvery, c.stallFor, c.lease, c.minMS, c.maxMS)
	fmt.Printf("        cancel=%.2f double_cancel=%.2f fail=%.2f long=%.3f burst=%.2f timeout_retry=%.2f dup=%.2f conflict=%.2f worker_args=%q\n",
		c.cancelFrac, c.doubleCancelFrac, c.failFrac, c.longFrac, c.burstFrac, c.retryFrac, c.dupFrac, c.conflictFrac, c.workerArgs)

	ctx := context.Background()
	if err := os.MkdirAll(c.logDir, 0o755); err != nil {
		fmt.Println("HARNESS ERROR:", err)
		return 2
	}
	var err error
	h.logFile, err = os.Create(filepath.Join(c.logDir, fmt.Sprintf("%s-%d.log", c.label, c.seed)))
	if err != nil {
		fmt.Println("HARNESS ERROR:", err)
		return 2
	}
	defer h.logFile.Close()

	h.pool, err = pgxpool.New(ctx, c.dbURL)
	if err != nil {
		fmt.Println("HARNESS ERROR:", err)
		return 2
	}
	defer h.pool.Close()
	if err := waitFor(30*time.Second, func() error { return h.pool.Ping(ctx) }); err != nil {
		fmt.Println("HARNESS ERROR: db:", err)
		return 2
	}
	if _, err := h.pool.Exec(ctx, `DROP TABLE IF EXISTS effects, job_runs, jobs CASCADE`); err != nil {
		fmt.Println("HARNESS ERROR: reset:", err)
		return 2
	}

	h.http = &http.Client{
		Timeout:   10 * time.Second,
		Transport: &http.Transport{MaxIdleConns: 256, MaxIdleConnsPerHost: 256, IdleConnTimeout: 30 * time.Second},
	}
	port, err := freePort()
	if err != nil {
		fmt.Println("HARNESS ERROR:", err)
		return 2
	}
	h.base = fmt.Sprintf("http://127.0.0.1:%d", port)
	h.api, err = h.spawn("api", c.apiBin, nil, "ADDR=127.0.0.1:"+fmt.Sprint(port))
	if err != nil {
		fmt.Println("HARNESS ERROR: api:", err)
		return 2
	}
	if err := waitFor(30*time.Second, func() error {
		resp, err := h.http.Get(h.base + "/healthz")
		if err != nil {
			return err
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent {
			return fmt.Errorf("healthz %d", resp.StatusCode)
		}
		return nil
	}); err != nil {
		fmt.Println("HARNESS ERROR: api not healthy:", err)
		return 2
	}

	h.workers = make([]*proc, c.workers)
	for i := range h.workers {
		if err := h.spawnWorker(i); err != nil {
			fmt.Println("HARNESS ERROR: worker:", err)
			return 2
		}
	}
	h.logf("api + %d workers up", c.workers)

	h.buildPlan()

	chaosCtx, stopChaos := context.WithCancel(ctx)
	var chaosWG sync.WaitGroup
	chaosWG.Add(3)
	go func() { defer chaosWG.Done(); h.killer(chaosCtx) }()
	go func() { defer chaosWG.Done(); h.staller(chaosCtx) }()
	go func() { defer chaosWG.Done(); h.observer(chaosCtx) }()
	go h.progress(chaosCtx)

	h.produce()
	h.logf("production done: %d keys submitted; waiting for async replays/cancels", c.jobs)
	h.pendingAsync.Wait()
	h.logf("async client traffic done; waiting for drain (no queued/running)")

	drained := h.waitDrain(ctx)
	stopChaos()
	chaosWG.Wait()
	// Resume any worker still frozen and give it time to attempt its stale write before we stop
	// everything; otherwise a zombie frozen at the very end is never tested.
	h.procMu.Lock()
	for _, p := range h.workers {
		if p != nil && !p.dead.Load() {
			_ = p.cmd.Process.Signal(syscall.SIGCONT)
		}
	}
	h.procMu.Unlock()
	time.Sleep(h.cfg.lease + time.Second)
	h.killEverything()
	elapsed := time.Since(h.start)
	h.logf("all processes killed; checking invariants (throughput: %.1f jobs/s end-to-end)", float64(c.jobs)/elapsed.Seconds())

	return h.verify(ctx, drained, elapsed)
}

func (h *harness) buildPlan() {
	c := h.cfg
	h.plan = make([]jobPlan, c.jobs)
	h.idByIdx = make([]int64, c.jobs)
	h.got201 = make([]int, c.jobs)
	h.cancelledOK = map[int64]bool{}
	h.observed = map[int64]obs{}
	for i := range h.plan {
		fail := h.chance(c.failFrac)
		dur := c.minMS + h.randN(c.maxMS-c.minMS+1)
		if h.chance(c.longFrac) {
			leaseMS := int(c.lease / time.Millisecond)
			dur = 2*leaseMS + h.randN(leaseMS+1)
		}
		p, _ := json.Marshal(map[string]any{"duration_ms": dur, "fail": fail, "n": i})
		h.plan[i] = jobPlan{key: fmt.Sprintf("job-%06d", i), payload: p, fail: fail}
	}
}

// ---------------------------------------------------------------- processes

func (h *harness) spawn(name, bin string, args []string, extraEnv ...string) (*proc, error) {
	cmd := exec.Command(bin, args...)
	cmd.Env = append(os.Environ(), "DATABASE_URL="+h.cfg.dbURL)
	cmd.Env = append(cmd.Env, extraEnv...)
	r, w, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	cmd.Stdout, cmd.Stderr = w, w
	if err := cmd.Start(); err != nil {
		w.Close()
		r.Close()
		return nil, err
	}
	w.Close()
	p := &proc{name: fmt.Sprintf("%s[pid=%d]", name, cmd.Process.Pid), cmd: cmd, done: make(chan struct{})}
	scanDone := make(chan struct{})
	go func() {
		defer close(scanDone)
		h.scan(name, r)
	}()
	go func() {
		_ = cmd.Wait()
		<-scanDone
		p.dead.Store(true)
		close(p.done)
	}()
	h.spawns.Add(1)
	return p, nil
}

func (h *harness) spawnWorker(slot int) error {
	args := strings.Fields(h.cfg.workerArgs)
	args = append(args, "-lease", h.cfg.lease.String())
	p, err := h.spawn("worker", h.cfg.workerBin, args)
	if err != nil {
		return err
	}
	h.procMu.Lock()
	h.workers[slot] = p
	h.procMu.Unlock()
	return nil
}

func (h *harness) scan(kind string, r io.ReadCloser) {
	defer r.Close()
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Text()
		h.logMu.Lock()
		fmt.Fprintf(h.logFile, "%s %s\n", kind, line)
		h.logMu.Unlock()
		switch {
		case strings.Contains(line, " CLAIM "):
			h.claims.Add(1)
		case strings.Contains(line, " DONE "):
			h.dones.Add(1)
			var job int64
			var att int32
			if i := strings.Index(line, "DONE "); i >= 0 {
				if n, _ := fmt.Sscanf(line[i:], "DONE job=%d attempt=%d", &job, &att); n == 2 {
					h.mu.Lock()
					h.doneJobs = append(h.doneJobs, job)
					h.doneAttempts = append(h.doneAttempts, att)
					h.mu.Unlock()
				}
			}
		case strings.Contains(line, " FENCED "):
			h.fenced.Add(1)
		case strings.Contains(line, " LEASE_LOST "):
			h.leaseLost.Add(1)
		case strings.Contains(line, " FAILED "):
			h.failedLines.Add(1)
		case strings.Contains(line, " REAP "):
			var rq, fl int64
			if i := strings.Index(line, "REAP "); i >= 0 {
				fmt.Sscanf(line[i:], "REAP requeued=%d failed=%d", &rq, &fl)
			}
			h.reapRequeued.Add(rq)
			h.reapFailed.Add(fl)
		case strings.Contains(line, " ERROR ") || strings.Contains(line, "level=ERROR"):
			if kind == "api" {
				h.apiErrs.Add(1)
			} else {
				h.workerErrs.Add(1)
			}
			h.mu.Lock()
			if len(h.errSamples) < 20 {
				h.errSamples = append(h.errSamples, kind+": "+line)
			}
			h.mu.Unlock()
		}
	}
}

// killer: kill -9 a random live worker roughly every killEvery, then respawn it so N stay alive.
func (h *harness) killer(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(h.jitter(h.cfg.killEvery)):
		}
		slot := h.randN(h.cfg.workers)
		h.procMu.Lock()
		p := h.workers[slot]
		h.procMu.Unlock()
		if p == nil || p.dead.Load() {
			continue
		}
		_ = p.cmd.Process.Signal(syscall.SIGKILL)
		<-p.done
		h.kills.Add(1)
		if err := h.spawnWorker(slot); err != nil {
			h.logf("HARNESS: respawn failed: %v", err)
		}
	}
}

// staller: freeze a worker longer than its lease (SIGSTOP), then resume it (SIGCONT).
// While frozen its lease expires, the job is reaped and given to another worker; on resume the
// frozen worker is a zombie still holding an old attempt number.
func (h *harness) staller(ctx context.Context) {
	if h.cfg.stallEvery <= 0 {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(h.jitter(h.cfg.stallEvery)):
		}
		slot := h.randN(h.cfg.workers)
		h.procMu.Lock()
		p := h.workers[slot]
		h.procMu.Unlock()
		if p == nil || p.dead.Load() || p.stopped.Load() {
			continue
		}
		if p.cmd.Process.Signal(syscall.SIGSTOP) != nil {
			continue
		}
		p.stopped.Store(true)
		h.stalls.Add(1)
		go func() {
			select {
			case <-time.After(h.cfg.stallFor):
			case <-p.done:
			case <-ctx.Done():
			}
			if !p.dead.Load() {
				_ = p.cmd.Process.Signal(syscall.SIGCONT)
			}
			p.stopped.Store(false)
		}()
	}
}

func (h *harness) killEverything() {
	h.procMu.Lock()
	procs := append([]*proc{h.api}, h.workers...)
	h.procMu.Unlock()
	for _, p := range procs {
		if p != nil && !p.dead.Load() {
			_ = p.cmd.Process.Signal(syscall.SIGKILL)
		}
	}
	for _, p := range procs {
		if p != nil {
			<-p.done
		}
	}
}

// ---------------------------------------------------------------- client traffic

type jobResp struct {
	ID      int64  `json:"id"`
	State   string `json:"state"`
	Attempt int    `json:"attempt"`
	Key     string `json:"idempotency_key"`
}

func (h *harness) post(key string, body []byte, timeout time.Duration) (int, *jobResp, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, h.base+"/jobs",
		bytes.NewReader([]byte(`{"payload":`+string(body)+`}`)))
	req.Header.Set("Idempotency-Key", key)
	req.Header.Set("Content-Type", "application/json")
	resp, err := h.http.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var j jobResp
	if resp.StatusCode == 200 || resp.StatusCode == 201 {
		if err := json.Unmarshal(raw, &j); err != nil {
			return resp.StatusCode, nil, err
		}
		return resp.StatusCode, &j, nil
	}
	return resp.StatusCode, nil, nil
}

// postReliable retries like a real client would on network errors and 5xx, always with the same key.
func (h *harness) postReliable(i int) (int, *jobResp) {
	pl := h.plan[i]
	for attempt := 0; attempt < 20; attempt++ {
		code, j, err := h.post(pl.key, pl.payload, 10*time.Second)
		if err != nil {
			h.httpNetErrs.Add(1)
			time.Sleep(time.Duration(50*(attempt+1)) * time.Millisecond)
			continue
		}
		if code >= 500 {
			h.http5xx.Add(1)
			time.Sleep(time.Duration(50*(attempt+1)) * time.Millisecond)
			continue
		}
		return code, j
	}
	return 0, nil
}

func (h *harness) record(i int, code int, j *jobResp, from string) {
	if code != 200 && code != 201 {
		h.violate("POST %s key=%s: unexpected status %d", from, h.plan[i].key, code)
		return
	}
	if code == 201 {
		h.posts201.Add(1)
	} else {
		h.posts200.Add(1)
	}
	if j.Key != h.plan[i].key {
		h.violate("POST %s key=%s returned job for key %s", from, h.plan[i].key, j.Key)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if code == 201 {
		h.got201[i]++
		if h.got201[i] > 1 {
			h.violations = append(h.violations, fmt.Sprintf("DOUBLE CREATE: key=%s got 201 %d times (%s)", h.plan[i].key, h.got201[i], from))
		}
	}
	switch prev := h.idByIdx[i]; {
	case prev == 0:
		h.idByIdx[i] = j.ID
		h.created = append(h.created, i)
	case prev != j.ID:
		h.violations = append(h.violations, fmt.Sprintf("DUPLICATE JOB: key=%s got id %d and id %d (%s)", h.plan[i].key, prev, j.ID, from))
	}
}

func (h *harness) produce() {
	c := h.cfg
	idx := make(chan int)
	var wg sync.WaitGroup
	for range c.producers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range idx {
				h.submit(i)
			}
		}()
	}
	var tick <-chan time.Time
	if c.rate > 0 {
		t := time.NewTicker(time.Duration(float64(time.Second) / c.rate))
		defer t.Stop()
		tick = t.C
	}
	for i := range c.jobs {
		if tick != nil {
			<-tick
		}
		idx <- i
	}
	close(idx)
	wg.Wait()
}

func (h *harness) submit(i int) {
	c := h.cfg
	switch {
	case h.chance(c.burstFrac):
		// 3 identical POSTs at the same instant: a double-click / racing retries
		h.bursts.Add(1)
		var wg sync.WaitGroup
		for range 3 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				code, j := h.postReliable(i)
				h.record(i, code, j, "burst")
			}()
		}
		wg.Wait()
	case h.chance(c.retryFrac):
		// client gives up after ~1ms (request may or may not have committed), then retries
		h.timeoutRetry.Add(1)
		_, _, _ = h.post(h.plan[i].key, h.plan[i].payload, time.Duration(1+h.randN(3))*time.Millisecond)
		code, j := h.postReliable(i)
		h.record(i, code, j, "retry-after-timeout")
	default:
		code, j := h.postReliable(i)
		h.record(i, code, j, "submit")
	}

	h.mu.Lock()
	id := h.idByIdx[i]
	h.mu.Unlock()
	if id == 0 {
		h.violate("LOST SUBMIT: key=%s never got a job id after retries", h.plan[i].key)
		return
	}
	if h.chance(c.dupFrac) {
		h.later(5*time.Second, func() {
			code, j := h.postReliable(i)
			h.dupReplays.Add(1)
			if code != 200 {
				h.violate("REPLAY key=%s: expected 200, got %d", h.plan[i].key, code)
				return
			}
			h.record(i, code, j, "replay")
		})
	}
	if h.chance(c.conflictFrac) {
		h.later(5*time.Second, func() {
			other := []byte(fmt.Sprintf(`{"duration_ms":1,"fail":false,"n":%d,"tampered":true}`, i))
			code, _, err := h.post(h.plan[i].key, other, 10*time.Second)
			if err == nil && code == 409 {
				h.conflictsOK.Add(1)
			} else {
				h.violate("CONFLICT key=%s with different payload: expected 409, got %d err=%v", h.plan[i].key, code, err)
			}
		})
	}
	if h.chance(c.cancelFrac) {
		double := h.chance(c.doubleCancelFrac)
		h.later(3*time.Second, func() {
			h.cancel(id)
			if double {
				h.cancel(id)
			}
		})
	}
}

func (h *harness) later(maxDelay time.Duration, f func()) {
	d := time.Duration(h.randN(int(maxDelay/time.Millisecond)+1)) * time.Millisecond
	h.pendingAsync.Add(1)
	go func() {
		defer h.pendingAsync.Done()
		time.Sleep(d)
		f()
	}()
}

func (h *harness) cancel(id int64) {
	h.cancelReqs.Add(1)
	for attempt := 0; attempt < 20; attempt++ {
		resp, err := h.http.Post(fmt.Sprintf("%s/jobs/%d/cancel", h.base, id), "application/json", nil)
		if err != nil {
			h.httpNetErrs.Add(1)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		switch resp.StatusCode {
		case 200:
			var j jobResp
			_ = json.Unmarshal(raw, &j)
			if j.State != "cancelled" {
				h.violate("CANCEL id=%d: 200 but state=%s", id, j.State)
			}
			h.cancel200.Add(1)
			h.mu.Lock()
			h.cancelledOK[id] = true
			h.mu.Unlock()
		case 409:
			h.cancel409.Add(1) // already succeeded/failed: legitimate
			h.mu.Lock()
			wasCancelled := h.cancelledOK[id]
			h.mu.Unlock()
			if wasCancelled {
				h.violate("CANCEL id=%d: second cancel got 409 after first returned cancelled", id)
			}
		default:
			if resp.StatusCode >= 500 {
				h.http5xx.Add(1)
				time.Sleep(100 * time.Millisecond)
				continue
			}
			h.violate("CANCEL id=%d: unexpected status %d", id, resp.StatusCode)
		}
		return
	}
	h.violate("CANCEL id=%d: gave up after retries", id)
}

// observer: GETs random jobs; a terminal state must never change, attempt must never decrease.
func (h *harness) observer(ctx context.Context) {
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				h.mu.Lock()
				n := len(h.created)
				var id int64
				if n > 0 {
					id = h.idByIdx[h.created[h.randN(n)]]
				}
				h.mu.Unlock()
				if id == 0 {
					time.Sleep(50 * time.Millisecond)
					continue
				}
				resp, err := h.http.Get(fmt.Sprintf("%s/jobs/%d", h.base, id))
				if err != nil {
					continue
				}
				var j jobResp
				code := resp.StatusCode
				_ = json.NewDecoder(resp.Body).Decode(&j)
				resp.Body.Close()
				if code >= 500 {
					h.http5xx.Add(1)
					continue
				}
				if code != 200 {
					h.violate("GET id=%d: status %d", id, code)
					continue
				}
				h.observations.Add(1)
				h.mu.Lock()
				prev := h.observed[id]
				if prev.terminal != "" && j.State != prev.terminal {
					h.violations = append(h.violations, fmt.Sprintf("TERMINAL FLIP id=%d: observed %s, later %s", id, prev.terminal, j.State))
				}
				if j.Attempt < prev.attempt {
					h.violations = append(h.violations, fmt.Sprintf("ATTEMPT WENT BACK id=%d: %d -> %d", id, prev.attempt, j.Attempt))
				}
				o := obs{terminal: prev.terminal, attempt: j.Attempt}
				if isTerminal(j.State) && o.terminal == "" {
					o.terminal = j.State
				}
				h.observed[id] = o
				h.mu.Unlock()
				time.Sleep(5 * time.Millisecond)
			}
		}()
	}
	wg.Wait()
}

func isTerminal(s string) bool { return s == "succeeded" || s == "failed" || s == "cancelled" }

func (h *harness) progress(ctx context.Context) {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		counts := h.stateCounts(context.Background())
		h.logf("progress: %v kills=%d stalls=%d claims=%d done=%d fenced=%d lease_lost=%d",
			counts, h.kills.Load(), h.stalls.Load(), h.claims.Load(), h.dones.Load(), h.fenced.Load(), h.leaseLost.Load())
	}
}

func (h *harness) stateCounts(ctx context.Context) map[string]int {
	out := map[string]int{}
	rows, err := h.pool.Query(ctx, `SELECT state, count(*) FROM jobs GROUP BY state`)
	if err != nil {
		return out
	}
	defer rows.Close()
	for rows.Next() {
		var s string
		var n int
		_ = rows.Scan(&s, &n)
		out[s] = n
	}
	return out
}

func (h *harness) waitDrain(ctx context.Context) bool {
	deadline := h.start.Add(h.cfg.timeout)
	for time.Now().Before(deadline) {
		var active int
		if err := h.pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE state IN ('queued','running')`).Scan(&active); err == nil && active == 0 {
			h.logf("drained")
			return true
		}
		time.Sleep(500 * time.Millisecond)
	}
	h.logf("DRAIN TIMEOUT after %s", h.cfg.timeout)
	return false
}

// ---------------------------------------------------------------- verification

type check struct {
	name   string
	bad    int
	detail []string
}

func (h *harness) verify(ctx context.Context, drained bool, elapsed time.Duration) int {
	var checks []*check
	add := func(name string, q string, args ...any) *check {
		c := &check{name: name}
		rows, err := h.pool.Query(ctx, q, args...)
		if err != nil {
			c.bad = -1
			c.detail = append(c.detail, "QUERY ERROR: "+err.Error())
			checks = append(checks, c)
			return c
		}
		defer rows.Close()
		for rows.Next() {
			vals, _ := rows.Values()
			c.bad++
			if len(c.detail) < 5 {
				c.detail = append(c.detail, fmt.Sprint(vals))
			}
		}
		checks = append(checks, c)
		return c
	}

	// --- 1. no job lost
	h.mu.Lock()
	ids := make([]int64, 0, len(h.idByIdx))
	keys := make([]string, 0, len(h.idByIdx))
	missingID := 0
	for i, id := range h.idByIdx {
		if id == 0 {
			missingID++
			continue
		}
		ids = append(ids, id)
		keys = append(keys, h.plan[i].key)
	}
	cancelled := make([]int64, 0, len(h.cancelledOK))
	for id := range h.cancelledOK {
		cancelled = append(cancelled, id)
	}
	failIDs := []int64{}
	for i, p := range h.plan {
		if p.fail && h.idByIdx[i] != 0 {
			failIDs = append(failIDs, h.idByIdx[i])
		}
	}
	h.mu.Unlock()

	lost := &check{name: "I1 no lost job: every acknowledged (key,id) exists in DB", bad: missingID}
	if missingID > 0 {
		lost.detail = append(lost.detail, fmt.Sprintf("%d keys never acknowledged", missingID))
	}
	checks = append(checks, lost)
	add("I1 no lost job: ack'd id matches DB row for that key",
		`SELECT k, i FROM unnest($1::text[], $2::bigint[]) AS a(k, i)
		 LEFT JOIN jobs j ON j.idempotency_key = a.k
		 WHERE j.id IS DISTINCT FROM a.i`, keys, ids)
	add("I1 no phantom/duplicate job: DB has exactly one row per submitted key",
		`SELECT count(*), count(DISTINCT idempotency_key), $1::int FROM jobs
		 HAVING count(*) <> $1 OR count(DISTINCT idempotency_key) <> $1`, h.cfg.jobs)

	// --- 2. at most one side effect
	add("I2 at most one effect per job",
		`SELECT job_id, count(*) FROM effects GROUP BY job_id HAVING count(*) > 1`)

	// --- 3. nothing stuck
	add("I3 nothing stuck in running/queued after drain",
		`SELECT id, state, attempt, lease_owner, lease_until FROM jobs WHERE state IN ('queued','running')`)

	// --- 4. final state consistency
	add("I4 succeeded <=> exactly one effect",
		`SELECT j.id, j.state, count(e.id) FROM jobs j LEFT JOIN effects e ON e.job_id = j.id
		 GROUP BY j.id, j.state
		 HAVING (j.state = 'succeeded') <> (count(e.id) = 1)`)
	add("I4 cancelled/failed jobs have no effect",
		`SELECT j.id, j.state FROM jobs j JOIN effects e ON e.job_id = j.id WHERE j.state IN ('cancelled','failed')`)
	add("I4 effect written by the job's final attempt (no stale/zombie effect)",
		`SELECT j.id, j.attempt, e.attempt FROM jobs j JOIN effects e ON e.job_id = j.id WHERE e.attempt <> j.attempt`)
	add("I4 cancel acknowledged with 200 => job is cancelled",
		`SELECT j.id, j.state FROM jobs j WHERE j.id = ANY($1) AND j.state <> 'cancelled'`, cancelled)
	add("I4 payload.fail=true => never succeeded",
		`SELECT id, state FROM jobs WHERE id = ANY($1) AND state = 'succeeded'`, failIDs)
	add("I4 attempt counter == number of claims, each attempt claimed once (no double claim)",
		`SELECT j.id, j.attempt, count(r.id), count(DISTINCT r.attempt), max(r.attempt)
		 FROM jobs j LEFT JOIN job_runs r ON r.job_id = j.id
		 GROUP BY j.id, j.attempt
		 HAVING count(r.id) <> j.attempt OR count(DISTINCT r.attempt) <> count(r.id) OR coalesce(max(r.attempt),0) <> j.attempt`)
	add("I4 terminal jobs have finished_at and no lease",
		`SELECT id, state FROM jobs WHERE state IN ('succeeded','failed','cancelled')
		 AND (finished_at IS NULL OR lease_owner IS NOT NULL OR lease_until IS NOT NULL)`)
	add("I4 non-failing payload only fails by exhausting attempts",
		`SELECT id, error FROM jobs WHERE state = 'failed' AND NOT (id = ANY($1)) AND error NOT LIKE 'lease expired%'`, failIDs)

	h.mu.Lock()
	doneJobs := append([]int64(nil), h.doneJobs...)
	doneAtt := append([]int32(nil), h.doneAttempts...)
	h.mu.Unlock()
	add("I7 every DONE a worker logged has the matching committed effect (no acknowledged-but-lost result)",
		`SELECT a.j, a.att FROM unnest($1::bigint[], $2::int[]) AS a(j, att)
		 LEFT JOIN effects e ON e.job_id = a.j AND e.attempt = a.att WHERE e.id IS NULL`, doneJobs, doneAtt)

	// Every lease expiry must be explained by a worker we killed or froze (a worker holds at most one
	// job). Unexplained expiries mean leases are lost without cause, e.g. a broken heartbeat; those
	// jobs would otherwise hide as "failed: max attempts exhausted".
	var expiries int
	_ = h.pool.QueryRow(ctx, `SELECT (SELECT count(*) - count(DISTINCT job_id) FROM job_runs)
		+ (SELECT count(*) FROM jobs WHERE state = 'failed' AND error LIKE 'lease expired%')`).Scan(&expiries)
	causes := int(h.kills.Load() + h.stalls.Load())
	ex := &check{name: "I8 lease expiries <= kills + stalls (no unexplained lease loss)"}
	if expiries > causes {
		ex.bad = expiries - causes
		ex.detail = append(ex.detail, fmt.Sprintf("expiries=%d kills+stalls=%d", expiries, causes))
	}
	checks = append(checks, ex)

	// Coverage: a run in which the chaos never actually hit anything proves nothing.
	cov := func(name string, ok bool, detail string) {
		c := &check{name: "C  coverage: " + name}
		if !ok {
			c.bad = 1
			c.detail = []string{detail}
		}
		checks = append(checks, c)
	}
	var multiRun, longFirstTry int
	_ = h.pool.QueryRow(ctx, `SELECT count(*) FROM (SELECT job_id FROM job_runs GROUP BY job_id HAVING count(*) > 1) x`).Scan(&multiRun)
	_ = h.pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE state = 'succeeded' AND attempt = 1
		AND (payload->>'duration_ms')::int > $1`, int(h.cfg.lease/time.Millisecond)).Scan(&longFirstTry)
	if h.cfg.killEvery > 0 {
		cov("kill -9 happened and some job ran more than once", h.kills.Load() > 0 && multiRun > 0,
			fmt.Sprintf("kills=%d multi-run jobs=%d", h.kills.Load(), multiRun))
	}
	if h.cfg.stallEvery > 0 {
		cov("a zombie was rejected (FENCED or LEASE_LOST)", h.fenced.Load()+h.leaseLost.Load() > 0,
			fmt.Sprintf("stalls=%d fenced=%d lease_lost=%d", h.stalls.Load(), h.fenced.Load(), h.leaseLost.Load()))
	}
	if h.cfg.longFrac > 0 {
		cov("heartbeat kept a job longer than the lease alive (succeeded on attempt 1)", longFirstTry > 0,
			fmt.Sprintf("long jobs succeeded first try=%d", longFirstTry))
	}
	if h.cfg.longFrac > 0 && h.cfg.cancelFrac > 0 {
		cov("a running job was cancelled and its worker aborted (LEASE_LOST)", h.leaseLost.Load() > 0,
			fmt.Sprintf("lease_lost=%d", h.leaseLost.Load()))
	}

	h.mu.Lock()
	clientViol := append([]string(nil), h.violations...)
	h.mu.Unlock()
	cats := map[string]int{}
	for _, v := range clientViol {
		cats[strings.SplitN(v, " ", 2)[0]]++
	}
	cv := &check{name: "I5 client-side: idempotency, 409 on conflict, cancel semantics, no terminal flip", bad: len(clientViol)}
	if len(cats) > 0 {
		cv.detail = append(cv.detail, fmt.Sprintf("by kind: %v", cats))
	}
	if len(clientViol) > 5 {
		cv.detail = append(cv.detail, clientViol[:5]...)
	} else {
		cv.detail = append(cv.detail, clientViol...)
	}
	checks = append(checks, cv)
	checks = append(checks, &check{name: "I6 no HTTP 5xx from API (availability, not data safety)", bad: int(h.http5xx.Load())})
	if !drained {
		checks = append(checks, &check{name: "drain finished before timeout", bad: 1})
	}

	// --- report
	fmt.Println()
	fmt.Println("=== STATS")
	fmt.Printf("elapsed=%s final_states=%v\n", elapsed.Round(time.Millisecond), h.stateCounts(ctx))
	fmt.Printf("chaos: kill9=%d sigstop_zombies=%d processes_spawned=%d\n", h.kills.Load(), h.stalls.Load(), h.spawns.Load())
	fmt.Printf("workers: claims=%d done=%d failed=%d fenced(stale completion rejected)=%d lease_lost(aborted)=%d reaped_requeued=%d reaped_failed=%d error_lines=%d\n",
		h.claims.Load(), h.dones.Load(), h.failedLines.Load(), h.fenced.Load(), h.leaseLost.Load(), h.reapRequeued.Load(), h.reapFailed.Load(), h.workerErrs.Load())
	fmt.Printf("http: post_201=%d post_200=%d bursts(3x)=%d timeout_then_retry=%d replays=%d conflicts_409=%d cancels=%d (200=%d 409=%d) gets=%d net_errs=%d 5xx=%d api_error_lines=%d\n",
		h.posts201.Load(), h.posts200.Load(), h.bursts.Load(), h.timeoutRetry.Load(), h.dupReplays.Load(), h.conflictsOK.Load(),
		h.cancelReqs.Load(), h.cancel200.Load(), h.cancel409.Load(), h.observations.Load(), h.httpNetErrs.Load(), h.http5xx.Load(), h.apiErrs.Load())

	var totalRuns, multi int
	_ = h.pool.QueryRow(ctx, `SELECT count(*) FROM job_runs`).Scan(&totalRuns)
	_ = h.pool.QueryRow(ctx, `SELECT count(*) FROM (SELECT job_id FROM job_runs GROUP BY job_id HAVING count(*) > 1) x`).Scan(&multi)
	fmt.Printf("executions: total_runs=%d jobs_executed_more_than_once=%d (at-least-once execution is expected; effects must still be <=1)\n", totalRuns, multi)
	rows, err := h.pool.Query(ctx, `SELECT runs, count(*) FROM (
		SELECT j.id, count(r.id) AS runs FROM jobs j LEFT JOIN job_runs r ON r.job_id = j.id GROUP BY j.id) x
		GROUP BY runs ORDER BY runs`)
	if err == nil {
		var parts []string
		for rows.Next() {
			var r, n int
			_ = rows.Scan(&r, &n)
			parts = append(parts, fmt.Sprintf("%d runs:%d jobs", r, n))
		}
		rows.Close()
		fmt.Printf("runs-per-job histogram: %s\n", strings.Join(parts, ", "))
	}
	var effects int
	_ = h.pool.QueryRow(ctx, `SELECT count(*) FROM effects`).Scan(&effects)
	var waitP50, waitP99, doneP50, doneP99 float64
	_ = h.pool.QueryRow(ctx, `
		SELECT coalesce(percentile_cont(0.5) WITHIN GROUP (ORDER BY w), 0), coalesce(percentile_cont(0.99) WITHIN GROUP (ORDER BY w), 0)
		FROM (SELECT extract(epoch FROM min(r.started_at) - j.created_at) AS w
		      FROM jobs j JOIN job_runs r ON r.job_id = j.id GROUP BY j.id) x`).Scan(&waitP50, &waitP99)
	_ = h.pool.QueryRow(ctx, `
		SELECT coalesce(percentile_cont(0.5) WITHIN GROUP (ORDER BY d), 0), coalesce(percentile_cont(0.99) WITHIN GROUP (ORDER BY d), 0)
		FROM (SELECT extract(epoch FROM finished_at - created_at) AS d FROM jobs WHERE state = 'succeeded') x`).Scan(&doneP50, &doneP99)
	fmt.Printf("latency: queued->first_claim p50=%.2fs p99=%.2fs | created->succeeded p50=%.2fs p99=%.2fs\n", waitP50, waitP99, doneP50, doneP99)
	fmt.Printf("effects: total=%d\n", effects)
	h.mu.Lock()
	if len(h.errSamples) > 0 {
		fmt.Println("error line samples:")
		for _, s := range h.errSamples {
			fmt.Println("  ", s)
		}
	}
	h.mu.Unlock()

	fmt.Println()
	fmt.Println("=== INVARIANTS")
	failed := 0
	sort.SliceStable(checks, func(i, j int) bool { return checks[i].name < checks[j].name })
	for _, c := range checks {
		status := "PASS"
		if c.bad != 0 {
			status = "FAIL"
			failed++
		}
		fmt.Printf("%s  %s", status, c.name)
		if c.bad != 0 {
			fmt.Printf("  (violations=%d)", c.bad)
		}
		fmt.Println()
		for _, d := range c.detail {
			fmt.Println("        ", d)
		}
	}
	fmt.Println()
	if failed > 0 {
		fmt.Printf("=== RESULT: FAIL (%d invariant groups violated) label=%s seed=%d\n", failed, h.cfg.label, h.cfg.seed)
		return 1
	}
	fmt.Printf("=== RESULT: PASS label=%s seed=%d\n", h.cfg.label, h.cfg.seed)
	return 0
}

// ---------------------------------------------------------------- utils

func waitFor(d time.Duration, f func() error) error {
	deadline := time.Now().Add(d)
	var err error
	for time.Now().Before(deadline) {
		if err = f(); err == nil {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return errors.Join(errors.New("timeout"), err)
}

func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
