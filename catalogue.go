package self

import (
	"mindseye/internal/model"
	"mindseye/internal/module"
)

// Metric names.
const (
	MetricFrameTime    = "frame.time"
	MetricFrameTimeMax = "frame.time_max"
	MetricHeap         = "memory.heap"
	MetricGoroutines   = "runtime.goroutines"
	MetricBusPublished = "bus.published"
	MetricBusDropped   = "bus.dropped"
	MetricLogMissed    = "log.missed"
)

// runtime/metrics names behind the runtime series.
const (
	nativeHeap       = "/memory/classes/heap/objects:bytes"
	nativeGoroutines = "/sched/goroutines:goroutines"
)

var catalogue = []module.Metric{
	{Name: MetricFrameTime, Unit: model.UnitSeconds, Kinds: []model.Kind{model.KindProcess}, Description: "mean time to draw a frame; no point while nothing is drawn"},
	{Name: MetricFrameTimeMax, Unit: model.UnitSeconds, Kinds: []model.Kind{model.KindProcess}, Description: "slowest frame"},
	{Name: MetricHeap, Unit: model.UnitBytes, Kinds: []model.Kind{model.KindProcess}, Description: "live heap objects", Native: nativeHeap},
	{Name: MetricGoroutines, Unit: model.UnitCount, Kinds: []model.Kind{model.KindProcess}, Description: "live goroutines", Native: nativeGoroutines},
	{Name: MetricBusPublished, Unit: model.UnitPerSec, Kinds: []model.Kind{KindBus}, Description: "events published on every topic"},
	{Name: MetricBusDropped, Unit: model.UnitPerSec, Kinds: []model.Kind{KindBus}, Description: "events a full subscriber missed"},
	{Name: MetricLogMissed, Unit: model.UnitPerSec, Kinds: []model.Kind{model.KindProcess}, Description: "log lines overwritten in the ring before they became events"},
}

func unitOf(metric string) (model.Unit, bool) {
	for _, m := range catalogue {
		if m.Name == metric {
			return m.Unit, true
		}
	}
	return "", false
}
