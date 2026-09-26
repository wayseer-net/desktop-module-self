package self

import (
	"mindseye/pkg/sdk"
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

var catalogue = []sdk.Metric{
	{Name: MetricFrameTime, Unit: sdk.UnitSeconds, Kinds: []sdk.Kind{sdk.KindProcess}, Description: "mean time to draw a frame; no point while nothing is drawn"},
	{Name: MetricFrameTimeMax, Unit: sdk.UnitSeconds, Kinds: []sdk.Kind{sdk.KindProcess}, Description: "slowest frame"},
	{Name: MetricHeap, Unit: sdk.UnitBytes, Kinds: []sdk.Kind{sdk.KindProcess}, Description: "live heap objects", Native: nativeHeap},
	{Name: MetricGoroutines, Unit: sdk.UnitCount, Kinds: []sdk.Kind{sdk.KindProcess}, Description: "live goroutines", Native: nativeGoroutines},
	{Name: MetricBusPublished, Unit: sdk.UnitPerSec, Kinds: []sdk.Kind{KindBus}, Description: "events published on every topic"},
	{Name: MetricBusDropped, Unit: sdk.UnitPerSec, Kinds: []sdk.Kind{KindBus}, Description: "events a full subscriber missed"},
	{Name: MetricLogMissed, Unit: sdk.UnitPerSec, Kinds: []sdk.Kind{sdk.KindProcess}, Description: "log lines overwritten in the ring before they became events"},
}

func unitOf(metric string) (sdk.Unit, bool) {
	for _, m := range catalogue {
		if m.Name == metric {
			return m.Unit, true
		}
	}
	return "", false
}
