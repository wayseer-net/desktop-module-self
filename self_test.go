package self

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"mindseye/pkg/sdk"
	"mindseye/pkg/sdk/sdktest"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"
)

// testProbe is a probe fed the way the app feeds it: bus traffic with drops, logs, modules
// and frames.
func testProbe(t *testing.T) (*Probe, *slog.Logger) {
	t.Helper()
	lines := &testLog{}
	log := slog.New(slog.NewTextHandler(lines, &slog.HandlerOptions{Level: slog.LevelDebug}))
	log.Info("started")
	log.Warn("disk nearly full", "free", "2%")
	p := NewProbe()
	p.SetBus(func() BusTraffic { return BusTraffic{Topics: 1, Published: 2, Dropped: 1} })
	p.SetLog(lines)
	p.SetModules(func() []ModuleState {
		return []ModuleState{
			{Name: "mindseye", Kind: "internal", State: sdk.FreshLive},
			{Name: "prom", Kind: "prometheus", State: sdk.FreshError, Err: "connection refused"},
		}
	})
	p.RecordFrame(4 * time.Millisecond)
	return p, log
}

// testLog keeps the lines written to it and hands them out as the app's log ring does.
type testLog struct {
	mu    sync.Mutex
	lines []string
}

func (l *testLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, strings.TrimSuffix(string(p), "\n"))
	return len(p), nil
}

func (l *testLog) Since(seq uint64) (lines []string, next uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := uint64(len(l.lines))
	return slices.Clone(l.lines[min(seq, n):]), n
}

func TestConformance(t *testing.T) {
	p, _ := testProbe(t)
	sdktest.Conform(t, sdktest.Case{
		New:     func() sdk.Module { return New(p) },
		Name:    "mindseye",
		Options: "interval: 20ms",
	})
}

// recSink records what a module sends.
type recSink struct {
	mu   sync.Mutex
	sets []sdk.ChangeSet
}

func (s *recSink) Snapshot(_ context.Context, cs *sdk.ChangeSet) error { return s.add(cs) }
func (s *recSink) Delta(_ context.Context, cs *sdk.ChangeSet) error    { return s.add(cs) }

func (s *recSink) add(cs *sdk.ChangeSet) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sets = append(s.sets, *cs)
	return nil
}

func (s *recSink) events() []sdk.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []sdk.Event
	for _, cs := range s.sets {
		out = append(out, cs.Events...)
	}
	return out
}

func (s *recSink) entity(ref sdk.EntityRef) (sdk.Entity, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var last sdk.Entity
	found := false
	for _, cs := range s.sets {
		for _, e := range cs.Upserts {
			if e.Ref == ref {
				last, found = e, true
			}
		}
	}
	return last, found
}

// running configures a module on p and runs it until the test ends.
func running(t *testing.T, p *Probe, options string) (*Module, *recSink) {
	t.Helper()
	m := New(p)
	if err := m.Configure(context.Background(), config(t, "mindseye", options)); err != nil {
		t.Fatal(err)
	}
	sink := &recSink{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- m.Run(ctx, sink) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("Run: %v", err)
		}
	})
	return m, sink
}

func config(t *testing.T, name sdk.ModuleID, options string) sdk.Config {
	t.Helper()
	var doc yaml.Node
	if err := yaml.Unmarshal([]byte(options), &doc); err != nil {
		t.Fatal(err)
	}
	c := sdk.Config{Name: name, Line: 1}
	if len(doc.Content) > 0 {
		c.Options = *doc.Content[0]
	}
	return c
}

// eventually polls cond until it holds or a second passes.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestFrameTimeSeriesAppearsAfterFramesAreRecorded(t *testing.T) {
	p := NewProbe()
	m, _ := running(t, p, "interval: 10ms")
	app := appRef("mindseye")
	now := time.Now()
	q := sdk.SeriesQuery{
		Entities: []sdk.EntityRef{app},
		Metrics:  []string{MetricFrameTime, MetricFrameTimeMax},
		Window:   sdk.TimeWindow{From: now.Add(-time.Minute), To: now.Add(time.Minute)},
	}
	series := func() []sdk.Series {
		got, err := m.QuerySeries(context.Background(), q)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	time.Sleep(30 * time.Millisecond)
	for _, s := range series() {
		if len(s.Points) > 0 {
			t.Fatalf("%s has points before any frame was drawn", s.Ref.Metric)
		}
	}
	p.RecordFrame(2 * time.Millisecond)
	p.RecordFrame(6 * time.Millisecond)
	eventually(t, "frame-time points", func() bool {
		got := series()
		return len(got) == 2 && len(got[0].Points) > 0 && len(got[1].Points) > 0
	})
	got := series()
	if v := got[0].Points[0].V; v != 0.004 {
		t.Errorf("mean frame time %v s, want 0.004", v)
	}
	if v := got[1].Points[0].V; v != 0.006 {
		t.Errorf("max frame time %v s, want 0.006", v)
	}
	if got[0].Unit != sdk.UnitSeconds {
		t.Errorf("unit %q", got[0].Unit)
	}
}

func TestRecordFrameDoesNotAllocate(t *testing.T) {
	p := NewProbe()
	if n := testing.AllocsPerRun(100, func() { p.RecordFrame(time.Millisecond) }); n != 0 {
		t.Errorf("RecordFrame allocates %v times", n)
	}
}

func TestLogLinesBecomeEventsOnceAcrossRestarts(t *testing.T) {
	p, log := testProbe(t)
	m := New(p)
	if err := m.Configure(context.Background(), config(t, "mindseye", "interval: 10ms")); err != nil {
		t.Fatal(err)
	}
	sink := &recSink{}
	runFor := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
		defer cancel()
		_ = m.Run(ctx, sink)
	}
	runFor()
	log.Error("module prom failed", "err", "timeout")
	runFor()

	evs := sink.events()
	msgs := make([]string, len(evs))
	for i, e := range evs {
		msgs[i] = e.Message
		if e.Source != "mindseye" || e.Entity != appRef("mindseye") || e.Kind != "log" || e.ID == "" {
			t.Errorf("event %+v", e)
		}
	}
	want := []string{"started", "disk nearly full free=2%", "module prom failed err=timeout"}
	if !slices.Equal(msgs, want) {
		t.Fatalf("messages %q, want %q", msgs, want)
	}
	if evs[1].Severity != sdk.SevWarn || evs[2].Severity != sdk.SevError {
		t.Errorf("severities %v, %v", evs[1].Severity, evs[2].Severity)
	}
	if time.Since(evs[0].At) > time.Minute {
		t.Errorf("event time %v not parsed from the log line", evs[0].At)
	}
}

func TestModulesBecomeEntitiesWithTheirFreshness(t *testing.T) {
	p, _ := testProbe(t)
	var mu sync.Mutex
	state := sdk.FreshError
	p.SetModules(func() []ModuleState {
		mu.Lock()
		defer mu.Unlock()
		return []ModuleState{{Name: "prom", Kind: "prometheus", State: state, Err: "connection refused", Note: "no journal"}}
	})
	_, sink := running(t, p, "interval: 10ms")
	ref := moduleRef("mindseye", "prom")
	eventually(t, "the prom entity", func() bool { _, ok := sink.entity(ref); return ok })
	e, _ := sink.entity(ref)
	if e.Status.Level != sdk.StatusCrit || e.Status.Reason != "connection refused" || e.Attrs["note"].Str() != "no journal" {
		t.Errorf("status %+v, attrs %v", e.Status, e.Attrs)
	}
	mu.Lock()
	state = sdk.FreshLive
	mu.Unlock()
	eventually(t, "prom to recover", func() bool { e, _ := sink.entity(ref); return e.Status.Level == sdk.StatusOK })
}

func TestSnapshotHasTheAppBusAndModules(t *testing.T) {
	p, _ := testProbe(t)
	m := New(p)
	if err := m.Configure(context.Background(), config(t, "mindseye", "")); err != nil {
		t.Fatal(err)
	}
	cs, err := m.Discover(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var refs []sdk.EntityRef
	for _, e := range cs.Upserts {
		refs = append(refs, e.Ref)
	}
	want := []sdk.EntityRef{appRef("mindseye"), busRef("mindseye"), moduleRef("mindseye", "mindseye"), moduleRef("mindseye", "prom")}
	if !slices.Equal(refs, want) {
		t.Errorf("entities %v, want %v", refs, want)
	}
	if len(cs.Edges) != 3 {
		t.Errorf("edges %v, want the bus and both modules running on the app", cs.Edges)
	}
}

func TestBadOptionsAreRejected(t *testing.T) {
	for _, opts := range []string{"interval: 1ms", "history: 1s", "interval: soon"} {
		if err := New(NewProbe()).Configure(context.Background(), config(t, "mindseye", opts)); err == nil {
			t.Errorf("%q accepted", opts)
		}
	}
}

func TestMemoryLimitIsReportedBesideTheHeap(t *testing.T) {
	was := debug.SetMemoryLimit(900 << 20)
	t.Cleanup(func() { debug.SetMemoryLimit(was) })
	m, _ := running(t, NewProbe(), "interval: 10ms")
	now := time.Now()
	q := sdk.SeriesQuery{
		Entities: []sdk.EntityRef{appRef("mindseye")},
		Metrics:  []string{MetricHeap, MetricRuntimeTotal, MetricMemoryLimit},
		Window:   sdk.TimeWindow{From: now.Add(-time.Minute), To: now.Add(time.Minute)},
	}
	var got []sdk.Series
	eventually(t, "memory points", func() bool {
		var err error
		if got, err = m.QuerySeries(context.Background(), q); err != nil {
			t.Fatal(err)
		}
		return len(got) == 3 && len(got[0].Points) > 0 && len(got[1].Points) > 0 && len(got[2].Points) > 0
	})
	heap, total, limit := got[0].Points[0].V, got[1].Points[0].V, got[2].Points[0].V
	if limit != 900<<20 || got[2].Unit != sdk.UnitBytes {
		t.Errorf("limit %v %s, want 900 MiB in bytes", limit, got[2].Unit)
	}
	if total < heap {
		t.Errorf("runtime total %v is less than the heap %v", total, heap)
	}
}

func TestNoMemoryLimitHasNoPoints(t *testing.T) {
	was := debug.SetMemoryLimit(math.MaxInt64)
	t.Cleanup(func() { debug.SetMemoryLimit(was) })
	m, _ := running(t, NewProbe(), "interval: 10ms")
	now := time.Now()
	q := sdk.SeriesQuery{
		Entities: []sdk.EntityRef{appRef("mindseye")},
		Metrics:  []string{MetricHeap, MetricMemoryLimit},
		Window:   sdk.TimeWindow{From: now.Add(-time.Minute), To: now.Add(time.Minute)},
	}
	var got []sdk.Series
	eventually(t, "heap points", func() bool {
		got, _ = m.QuerySeries(context.Background(), q)
		return len(got) == 2 && len(got[0].Points) > 1
	})
	if len(got[1].Points) > 0 {
		t.Errorf("memory.limit has points %v with no limit set", got[1].Points)
	}
}
