package self

import (
	"maps"
	"mindseye/internal/data"
	"mindseye/internal/model"
	"os"
	"runtime"
	"runtime/metrics"
	"slices"
	"strconv"
	"time"
)

// Entity kinds the module adds to the core vocabulary; the app itself is a core process.
const (
	KindBus    model.Kind = "mindseye/bus"
	KindModule model.Kind = "mindseye/module"
)

// started approximates when the process started.
var started = time.Now()

func appRef(src model.ModuleID) model.EntityRef {
	return mustRef(src, model.KindProcess, "mindseye")
}

func busRef(src model.ModuleID) model.EntityRef { return mustRef(src, KindBus, "bus") }

func moduleRef(src, name model.ModuleID) model.EntityRef {
	return mustRef(src, KindModule, string(name))
}

// mustRef builds a ref from parts that are valid by construction; config names are checked.
func mustRef(src model.ModuleID, kind model.Kind, native string) model.EntityRef {
	r, err := model.NewEntityRef(string(src), kind, native)
	if err != nil {
		panic(err)
	}
	return r
}

// world lists the entities and edges the module owns now: the app, then the rest by ref.
func (m *Module) world() ([]model.Entity, []model.Edge) {
	app := m.entity(appRef(m.name), model.KindProcess, "Mind's Eye", model.Status{Level: model.StatusOK}, map[string]model.Value{
		"pid":        model.Number(float64(os.Getpid())),
		"go_version": model.String(runtime.Version()),
		"version":    model.String(version()),
		"started":    model.Time(started),
	})
	ents := []model.Entity{app}
	if bus := m.probe.bus.Load(); bus != nil {
		attrs := map[string]model.Value{"topics": model.Number(float64(len(bus.Stats())))}
		ents = append(ents, m.entity(busRef(m.name), KindBus, "event bus", model.Status{Level: model.StatusOK}, attrs))
	}
	for _, s := range m.probe.moduleStates() {
		if s.Name.Validate() != nil {
			continue
		}
		attrs := map[string]model.Value{"kind": model.String(s.Kind), "freshness": model.String(s.State.String())}
		ents = append(ents, m.entity(moduleRef(m.name, s.Name), KindModule, string(s.Name), moduleStatus(s), attrs))
	}
	slices.SortFunc(ents[1:], func(a, b model.Entity) int { return compareRefs(a.Ref, b.Ref) })
	var edges []model.Edge
	for _, e := range ents[1:] {
		edges = append(edges, model.Edge{From: e.Ref, To: app.Ref, Rel: model.RelRunsOn, Weight: 1, Source: m.name})
	}
	return ents, edges
}

func (m *Module) entity(ref model.EntityRef, kind model.Kind, name string, st model.Status, attrs map[string]model.Value) model.Entity {
	return model.Entity{Ref: ref, Kind: kind, Name: name, Status: st, Attrs: attrs, Source: m.name}
}

// moduleStatus maps freshness to entity health.
func moduleStatus(s ModuleState) model.Status {
	switch s.State {
	case data.FreshLive:
		return model.Status{Level: model.StatusOK}
	case data.FreshStale:
		return model.Status{Level: model.StatusWarn, Reason: "no recent data"}
	case data.FreshError:
		return model.Status{Level: model.StatusCrit, Reason: s.Err}
	}
	return model.Status{Level: model.StatusDown, Reason: "disconnected"}
}

// worldChanges returns the entities that differ from what was sent and records them as sent.
func (m *Module) worldChanges(now time.Time) *model.ChangeSet {
	ents, edges := m.world()
	cs := &model.ChangeSet{}
	added := map[model.EntityRef]bool{}
	for _, e := range ents {
		old, ok := m.sent[e.Ref]
		if ok && sameEntity(&old, &e) {
			continue
		}
		added[e.Ref] = !ok
		m.sent[e.Ref] = e
		e.Seen = now
		cs.Upserts = append(cs.Upserts, e)
	}
	for _, e := range edges {
		if added[e.From] {
			cs.Edges = append(cs.Edges, e)
		}
	}
	for _, r := range sortedRefs(m.sent) {
		if !slices.ContainsFunc(ents, func(e model.Entity) bool { return e.Ref == r }) {
			delete(m.sent, r)
			cs.Removes = append(cs.Removes, r)
		}
	}
	return cs
}

// sameEntity compares everything the module sets, which excludes Seen.
func sameEntity(a, b *model.Entity) bool {
	return a.Name == b.Name && a.Status == b.Status && maps.EqualFunc(a.Attrs, b.Attrs, model.Value.Equal)
}

func sortedRefs[V any](m map[model.EntityRef]V) []model.EntityRef {
	return slices.SortedFunc(maps.Keys(m), compareRefs)
}

func compareRefs(a, b model.EntityRef) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// sample records one point of every series; rates need a previous tick.
func (m *Module) sample(now time.Time, logMissed uint64) {
	elapsed := now.Sub(m.last)
	first := m.last.IsZero()
	m.last = now
	app := appRef(m.name)
	if n, mean, slowest := m.probe.frames.take(); n > 0 {
		m.record(app, MetricFrameTime, now, mean.Seconds())
		m.record(app, MetricFrameTimeMax, now, slowest.Seconds())
	}
	metrics.Read(m.runtime)
	m.record(app, MetricHeap, now, float64(m.runtime[0].Value.Uint64()))
	m.record(app, MetricGoroutines, now, float64(m.runtime[1].Value.Uint64()))
	if !first {
		m.record(app, MetricLogMissed, now, float64(logMissed)/elapsed.Seconds())
	}
	if bus := m.probe.bus.Load(); bus != nil {
		var published, dropped uint64
		for _, t := range bus.Stats() {
			published += t.Published
			dropped += t.Dropped
		}
		m.rate(busRef(m.name), MetricBusPublished, now, elapsed, published, first)
		m.rate(busRef(m.name), MetricBusDropped, now, elapsed, dropped, first)
	}
}

// rate records the per-second growth of a cumulative total since the previous tick.
func (m *Module) rate(ref model.EntityRef, metric string, now time.Time, elapsed time.Duration, total uint64, first bool) {
	prev, seen := m.counters[metric]
	m.counters[metric] = total
	if seen && !first && total >= prev {
		m.record(ref, metric, now, float64(total-prev)/elapsed.Seconds())
	}
}

func (m *Module) record(ref model.EntityRef, metric string, now time.Time, v float64) {
	key := data.SeriesRef{Entity: ref, Metric: metric}
	h := m.series[key]
	if h == nil {
		h = newHistory(int(m.opts.History / m.opts.Interval))
		m.series[key] = h
	}
	h.add(data.Point{T: now.UnixNano(), V: v})
}

// newEvents turns log lines written since the last call into events.
func (m *Module) newEvents(now time.Time) (events []model.Event, missed uint64) {
	ring := m.probe.log.Load()
	if ring == nil {
		return nil, 0
	}
	lines, next := ring.Since(m.logNext)
	first := next - uint64(len(lines))
	missed, m.logNext = first-m.logNext, next
	for i, line := range lines {
		at, sev, msg := parseLogLine(line)
		if at.IsZero() {
			at = now
		}
		events = append(events, model.Event{
			ID: "log-" + strconv.FormatUint(first+uint64(i), 10), Entity: appRef(m.name), At: at,
			Severity: sev, Kind: "log", Message: msg, Source: m.name,
		})
	}
	m.events = append(m.events, events...)
	if over := len(m.events) - eventCap; over > 0 {
		m.events = slices.Delete(m.events, 0, over)
	}
	return events, missed
}
