package self

import (
	"maps"
	"math"
	"mindseye/pkg/sdk"
	"os"
	"runtime"
	"runtime/metrics"
	"slices"
	"strconv"
	"time"
)

// Entity kinds the module adds to the core vocabulary; the app itself is a core process.
const (
	KindBus    sdk.Kind = "mindseye/bus"
	KindModule sdk.Kind = "mindseye/module"
)

// started approximates when the process started.
var started = time.Now()

func appRef(src sdk.ModuleID) sdk.EntityRef {
	return mustRef(src, sdk.KindProcess, "mindseye")
}

func busRef(src sdk.ModuleID) sdk.EntityRef { return mustRef(src, KindBus, "bus") }

func moduleRef(src, name sdk.ModuleID) sdk.EntityRef {
	return mustRef(src, KindModule, string(name))
}

// mustRef builds a ref from parts that are valid by construction; config names are checked.
func mustRef(src sdk.ModuleID, kind sdk.Kind, native string) sdk.EntityRef {
	r, err := sdk.NewEntityRef(string(src), kind, native)
	if err != nil {
		panic(err)
	}
	return r
}

// world lists the entities and edges the module owns now: the app, then the rest by ref.
func (m *Module) world() ([]sdk.Entity, []sdk.Edge) {
	app := m.entity(appRef(m.name), sdk.KindProcess, "Mind's Eye", sdk.Status{Level: sdk.StatusOK}, map[string]sdk.Value{
		"pid":        sdk.Number(float64(os.Getpid())),
		"go_version": sdk.String(runtime.Version()),
		"version":    sdk.String(version()),
		"started":    sdk.Time(started),
	})
	ents := []sdk.Entity{app}
	if bus, ok := m.probe.busTraffic(); ok {
		attrs := map[string]sdk.Value{"topics": sdk.Number(float64(bus.Topics))}
		ents = append(ents, m.entity(busRef(m.name), KindBus, "event bus", sdk.Status{Level: sdk.StatusOK}, attrs))
	}
	for _, s := range m.probe.moduleStates() {
		if s.Name.Validate() != nil {
			continue
		}
		attrs := map[string]sdk.Value{"kind": sdk.String(s.Kind), "freshness": sdk.String(freshness(s))}
		if s.Note != "" {
			attrs["note"] = sdk.String(s.Note)
		}
		ents = append(ents, m.entity(moduleRef(m.name, s.Name), KindModule, string(s.Name), moduleStatus(s), attrs))
	}
	slices.SortFunc(ents[1:], func(a, b sdk.Entity) int { return compareRefs(a.Ref, b.Ref) })
	var edges []sdk.Edge
	for _, e := range ents[1:] {
		edges = append(edges, sdk.Edge{From: e.Ref, To: app.Ref, Rel: sdk.RelRunsOn, Weight: 1, Source: m.name})
	}
	return ents, edges
}

func (m *Module) entity(ref sdk.EntityRef, kind sdk.Kind, name string, st sdk.Status, attrs map[string]sdk.Value) sdk.Entity {
	return sdk.Entity{Ref: ref, Kind: kind, Name: name, Status: st, Attrs: attrs, Source: m.name}
}

// moduleStatus maps freshness to entity health; a module turned off is not failing.
func moduleStatus(s ModuleState) sdk.Status {
	if s.Off {
		return sdk.Status{Level: sdk.StatusUnknown, Reason: "turned off"}
	}
	switch s.State {
	case sdk.FreshLive:
		return sdk.Status{Level: sdk.StatusOK}
	case sdk.FreshStale:
		return sdk.Status{Level: sdk.StatusWarn, Reason: "no recent data"}
	case sdk.FreshError:
		return sdk.Status{Level: sdk.StatusCrit, Reason: s.Err}
	}
	return sdk.Status{Level: sdk.StatusDown, Reason: "disconnected"}
}

// freshness is s's freshness as its attribute says it, or "off".
func freshness(s ModuleState) string {
	if s.Off {
		return "off"
	}
	return s.State.String()
}

// worldChanges returns the entities that differ from what was sent and records them as sent.
func (m *Module) worldChanges(now time.Time) *sdk.ChangeSet {
	ents, edges := m.world()
	cs := &sdk.ChangeSet{}
	added := map[sdk.EntityRef]bool{}
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
		if !slices.ContainsFunc(ents, func(e sdk.Entity) bool { return e.Ref == r }) {
			delete(m.sent, r)
			cs.Removes = append(cs.Removes, r)
		}
	}
	return cs
}

// sameEntity compares everything the module sets, which excludes Seen.
func sameEntity(a, b *sdk.Entity) bool {
	return a.Name == b.Name && a.Status == b.Status && maps.EqualFunc(a.Attrs, b.Attrs, sdk.Value.Equal)
}

func sortedRefs[V any](m map[sdk.EntityRef]V) []sdk.EntityRef {
	return slices.SortedFunc(maps.Keys(m), compareRefs)
}

func compareRefs(a, b sdk.EntityRef) int {
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
	for r := range FrameReasons {
		m.rate(app, frameRateOf(r), now, elapsed, m.probe.why[r].Load(), first)
	}
	metrics.Read(m.runtime)
	m.record(app, MetricHeap, now, float64(m.runtime[0].Value.Uint64()))
	m.record(app, MetricGoroutines, now, float64(m.runtime[1].Value.Uint64()))
	m.record(app, MetricRuntimeTotal, now, float64(m.runtime[2].Value.Uint64()))
	if limit := m.runtime[3].Value.Uint64(); limit < math.MaxInt64 {
		m.record(app, MetricMemoryLimit, now, float64(limit))
	}
	if !first {
		m.record(app, MetricLogMissed, now, float64(logMissed)/elapsed.Seconds())
	}
	if bus, ok := m.probe.busTraffic(); ok {
		m.rate(busRef(m.name), MetricBusPublished, now, elapsed, bus.Published, first)
		m.rate(busRef(m.name), MetricBusDropped, now, elapsed, bus.Dropped, first)
	}
}

// rate records the per-second growth of a cumulative total since the previous tick.
func (m *Module) rate(ref sdk.EntityRef, metric string, now time.Time, elapsed time.Duration, total uint64, first bool) {
	prev, seen := m.counters[metric]
	m.counters[metric] = total
	if seen && !first && total >= prev {
		m.record(ref, metric, now, float64(total-prev)/elapsed.Seconds())
	}
}

func (m *Module) record(ref sdk.EntityRef, metric string, now time.Time, v float64) {
	key := sdk.SeriesRef{Entity: ref, Metric: metric}
	h := m.series[key]
	if h == nil {
		h = sdk.NewRing(int(m.opts.History / m.opts.Interval))
		m.series[key] = h
	}
	h.Add(sdk.Point{T: now.UnixNano(), V: v})
}

// newEvents turns log lines written since the last call, and reported events, into events.
func (m *Module) newEvents(now time.Time) (events []sdk.Event, missed uint64) {
	if src := m.probe.log.Load(); src != nil {
		events, missed = m.logEvents(*src, now)
	}
	for _, e := range m.probe.takeReported() {
		m.reportNext++
		e.ID, e.Source = "report-"+strconv.FormatUint(m.reportNext, 10), m.name
		if e.At.IsZero() {
			e.At = now
		}
		events = append(events, e)
	}
	m.events = append(m.events, events...)
	if over := len(m.events) - eventCap; over > 0 {
		m.events = slices.Delete(m.events, 0, over)
	}
	return events, missed
}

func (m *Module) logEvents(src LogSource, now time.Time) (events []sdk.Event, missed uint64) {
	lines, next := src.Since(m.logNext)
	first := next - uint64(len(lines))
	missed, m.logNext = first-m.logNext, next
	for i, line := range lines {
		at, sev, msg := parseLogLine(line)
		if at.IsZero() {
			at = now
		}
		events = append(events, sdk.Event{
			ID: "log-" + strconv.FormatUint(first+uint64(i), 10), Entity: appRef(m.name), At: at,
			Severity: sev, Kind: "log", Message: msg, Source: m.name,
		})
	}
	return events, missed
}
