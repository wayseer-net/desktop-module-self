package self

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"runtime/metrics"
	"slices"
	"strings"
	"sync"
	"time"
	"wayseer/pkg/sdk"
)

// Kind is the module kind in config.
const Kind = "internal"

// eventCap bounds the log events kept for queries.
const eventCap = 2000

func init() { sdk.Register(Kind, func() sdk.Module { return New(Default) }) }

type options struct {
	Interval time.Duration `yaml:"interval"` // how often Mind's Eye samples itself; default 1s
	History  time.Duration `yaml:"history"`  // how far back series are kept, 10 intervals to 24h; default 1h
}

func (o options) validate() error {
	switch {
	case o.Interval < 10*time.Millisecond:
		return fmt.Errorf("interval %v is below 10ms", o.Interval)
	case o.History < 10*o.Interval || o.History > 24*time.Hour:
		return fmt.Errorf("history %v must be at least 10 intervals and at most 24h", o.History)
	}
	return nil
}

// Module reports the running app to itself.
type Module struct {
	probe *Probe
	name  sdk.ModuleID
	opts  options

	mu         sync.Mutex // guards what follows, shared by Run and queries
	series     map[sdk.SeriesRef]*sdk.Ring
	sent       map[sdk.EntityRef]sdk.Entity // entities as last sent, without Seen
	events     []sdk.Event                  // newest last, at most eventCap
	logNext    uint64
	reportNext uint64            // reported events so far, for their IDs
	counters   map[string]uint64 // cumulative totals behind the rate series
	runtime    []metrics.Sample
	last       time.Time // the previous tick
}

// New makes an unconfigured module reading p.
func New(p *Probe) *Module { return &Module{probe: p} }

// Info describes the module.
func (m *Module) Info() sdk.Info {
	return sdk.Info{Kind: Kind, Version: version(), Description: "Mind's Eye itself: frame time, memory, the event bus, modules and the log"}
}

// Configure decodes options and resets the history.
func (m *Module) Configure(_ context.Context, cfg sdk.Config) error {
	o := options{Interval: time.Second, History: time.Hour}
	if err := cfg.Decode(&o); err != nil {
		return err
	}
	if err := o.validate(); err != nil {
		return fmt.Errorf("line %d: %w", cfg.Line, err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.name, m.opts = cfg.Name, o
	m.series = map[sdk.SeriesRef]*sdk.Ring{}
	m.sent = map[sdk.EntityRef]sdk.Entity{}
	m.events, m.logNext, m.counters = nil, 0, map[string]uint64{}
	m.runtime = []metrics.Sample{{Name: nativeHeap}, {Name: nativeGoroutines}, {Name: nativeTotal}, {Name: nativeLimit}}
	m.last = time.Time{}
	return nil
}

// Run sends a snapshot, then samples every interval until ctx ends.
func (m *Module) Run(ctx context.Context, sink sdk.Sink) error {
	m.mu.Lock()
	clear(m.sent) // a snapshot resends everything
	m.mu.Unlock()
	if err := sink.Snapshot(ctx, m.tick(time.Now())); err != nil {
		return err
	}
	t := time.NewTicker(m.opts.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case now := <-t.C:
			if err := sink.Delta(ctx, m.tick(now)); err != nil { // empty still says it is live
				return err
			}
		}
	}
}

// Health is always good: the source is the process itself.
func (m *Module) Health() sdk.Health { return sdk.Health{} }

// tick samples every series and returns what changed in the world since the last tick.
func (m *Module) tick(now time.Time) *sdk.ChangeSet {
	m.mu.Lock()
	defer m.mu.Unlock()
	events, missed := m.newEvents(now)
	m.sample(now, missed)
	cs := m.worldChanges(now)
	cs.Events = events
	return cs
}

// Discover returns the module's whole state without changing what Run has sent.
func (m *Module) Discover(ctx context.Context) (*sdk.ChangeSet, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	ents, edges := m.world()
	m.mu.Unlock()
	now := time.Now()
	for i := range ents {
		ents[i].Seen = now
	}
	return &sdk.ChangeSet{Upserts: ents, Edges: edges}, nil
}

// Metrics lists the series the module records.
func (m *Module) Metrics() []sdk.Metric { return slices.Clone(catalogue) }

// QuerySeries answers from the recorded history, thinned to about one point per step.
func (m *Module) QuerySeries(ctx context.Context, q sdk.SeriesQuery) ([]sdk.Series, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []sdk.Series
	for _, ref := range m.queried(q) {
		unit, _ := unitOf(ref.Metric)
		s := sdk.Series{Ref: ref, Unit: unit}
		if h := m.series[ref]; h != nil {
			s.Points = thin(h.In(q.Window), q)
		}
		out = append(out, s)
	}
	return out, nil
}

// queried lists the asked-for series that exist, in query order.
func (m *Module) queried(q sdk.SeriesQuery) []sdk.SeriesRef {
	var out []sdk.SeriesRef
	for _, e := range m.matching(q) {
		for _, name := range q.Metrics {
			i := slices.IndexFunc(catalogue, func(c sdk.Metric) bool { return c.Name == name })
			if i >= 0 && slices.Contains(catalogue[i].Kinds, e.Kind) {
				out = append(out, sdk.SeriesRef{Entity: e.Ref, Metric: name})
			}
		}
	}
	return out
}

func (m *Module) matching(q sdk.SeriesQuery) []sdk.Entity {
	var out []sdk.Entity
	if len(q.Entities) > 0 {
		for _, r := range q.Entities {
			if e, ok := m.sent[r]; ok {
				out = append(out, e)
			}
		}
		return out
	}
	for _, r := range sortedRefs(m.sent) {
		if e := m.sent[r]; q.Filter.Match(&e) {
			out = append(out, e)
		}
	}
	return out
}

func thin(ps []sdk.Point, q sdk.SeriesQuery) []sdk.Point {
	if q.Step <= 0 {
		return ps
	}
	return sdk.Downsample(ps, q.Window, int(q.Window.Span()/q.Step))
}

// QueryEvents answers from the log events kept so far.
func (m *Module) QueryEvents(ctx context.Context, q sdk.EventQuery) ([]sdk.Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []sdk.Event
	for i := len(m.events) - 1; i >= 0 && (q.Limit == 0 || len(out) < q.Limit); i-- {
		if e := m.events[i]; eventMatches(&e, q) {
			out = append(out, e)
		}
	}
	slices.Reverse(out)
	return out, nil
}

func eventMatches(e *sdk.Event, q sdk.EventQuery) bool {
	switch {
	case e.Severity < q.MinSeverity:
		return false
	case len(q.Entities) > 0 && !slices.Contains(q.Entities, e.Entity):
		return false
	case len(q.Kinds) > 0 && !slices.Contains(q.Kinds, e.Kind):
		return false
	}
	return q.Window.From.IsZero() && q.Window.To.IsZero() || q.Window.Contains(e.At.UnixNano())
}

// Search finds the module's entities whose name contains text, ignoring case.
func (m *Module) Search(ctx context.Context, text string, limit int) ([]sdk.EntityRef, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, errors.New("search limit must be positive")
	}
	m.mu.Lock()
	ents, _ := m.world()
	m.mu.Unlock()
	text = strings.ToLower(text)
	var out []sdk.EntityRef
	for _, e := range ents {
		if len(out) < limit && strings.Contains(strings.ToLower(e.Name), text) {
			out = append(out, e.Ref)
		}
	}
	return out, nil
}

// version is the app's module version, or "devel" in a build without one.
func version() string {
	if bi, ok := debug.ReadBuildInfo(); ok && bi.Main.Version != "" && bi.Main.Version != "(devel)" {
		return bi.Main.Version
	}
	return "devel"
}
