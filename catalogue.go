package self

import (
	"mindseye/pkg/sdk"
)

// Metric names.
const (
	MetricFrameTime    = "frame.time"
	MetricFrameTimeMax = "frame.time_max"
	MetricFrameRate    = "frame.rate" // one metric per reason, such as frame.rate.live
	MetricHeap         = "memory.heap"
	MetricRuntimeTotal = "memory.runtime"
	MetricMemoryLimit  = "memory.limit"
	MetricGoroutines   = "runtime.goroutines"
	MetricBusPublished = "bus.published"
	MetricBusDropped   = "bus.dropped"
	MetricLogMissed    = "log.missed"
)

// runtime/metrics names behind the runtime series.
const (
	nativeHeap       = "/memory/classes/heap/objects:bytes"
	nativeGoroutines = "/sched/goroutines:goroutines"
	nativeTotal      = "/memory/classes/total:bytes"
	nativeLimit      = "/gc/gomemlimit:bytes"
)

var catalogue = append([]sdk.Metric{
	{Name: MetricFrameTime, Unit: sdk.UnitSeconds, Kinds: []sdk.Kind{sdk.KindProcess}, Description: "mean time to draw a frame; no point while nothing is drawn"},
	{Name: MetricFrameTimeMax, Unit: sdk.UnitSeconds, Kinds: []sdk.Kind{sdk.KindProcess}, Description: "slowest frame"},
	{Name: MetricHeap, Unit: sdk.UnitBytes, Kinds: []sdk.Kind{sdk.KindProcess}, Description: "live heap objects", Native: nativeHeap},
	{Name: MetricRuntimeTotal, Unit: sdk.UnitBytes, Kinds: []sdk.Kind{sdk.KindProcess}, Description: "all memory the Go runtime holds, which memory.limit bounds", Native: nativeTotal},
	{Name: MetricMemoryLimit, Unit: sdk.UnitBytes, Kinds: []sdk.Kind{sdk.KindProcess}, Description: "the config's memory_limit, past which the GC works harder", Native: nativeLimit},
	{Name: MetricGoroutines, Unit: sdk.UnitCount, Kinds: []sdk.Kind{sdk.KindProcess}, Description: "live goroutines", Native: nativeGoroutines},
	{Name: MetricBusPublished, Unit: sdk.UnitPerSec, Kinds: []sdk.Kind{KindBus}, Description: "events published on every topic"},
	{Name: MetricBusDropped, Unit: sdk.UnitPerSec, Kinds: []sdk.Kind{KindBus}, Description: "events a full subscriber missed"},
	{Name: MetricLogMissed, Unit: sdk.UnitPerSec, Kinds: []sdk.Kind{sdk.KindProcess}, Description: "log lines overwritten in the ring before they became events"},
}, frameRates()...)

var frameRateHelp = [FrameReasons]string{
	"frames drawn for a key, the pointer, the window, or a reply to them",
	"frames drawn while something moves",
	"frames drawn because a module changed the world",
	"frames drawn because series or events arrived",
	"frames drawn to move a live time bar on",
	"frames drawn because the desktop's look changed",
}

// frameRates are the frame.rate metrics, one per reason.
func frameRates() []sdk.Metric {
	ms := make([]sdk.Metric, FrameReasons)
	for r := range FrameReasons {
		ms[r] = sdk.Metric{Name: frameRateOf(r), Unit: sdk.UnitPerSec, Kinds: []sdk.Kind{sdk.KindProcess}, Description: frameRateHelp[r]}
	}
	return ms
}

// frameRateOf names r's frame.rate metric, without allocating on each sample.
func frameRateOf(r FrameReason) string { return frameRateNames[r] }

var frameRateNames = func() (n [FrameReasons]string) {
	for r := range FrameReasons {
		n[r] = MetricFrameRate + "." + r.String()
	}
	return n
}()

func unitOf(metric string) (sdk.Unit, bool) {
	for _, m := range catalogue {
		if m.Name == metric {
			return m.Unit, true
		}
	}
	return "", false
}
