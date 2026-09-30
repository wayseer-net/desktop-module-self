package self

import (
	"mindseye/pkg/sdk"
	"sync"
	"sync/atomic"
	"time"
)

// ModuleState is one module instance as the app sees it.
type ModuleState struct {
	Name  sdk.ModuleID
	Kind  string
	State sdk.State
	Err   string // the module's health error; module errors never carry secrets
	Note  string // a limit the module reports that is not an error
}

// Probe is what the app knows about itself. The app sets its parts; the module reads them.
type Probe struct {
	frames  frameStats
	why     [FrameReasons]atomic.Uint64 // frames drawn for each reason
	bus     atomic.Pointer[func() BusTraffic]
	log     atomic.Pointer[LogSource]
	modules atomic.Pointer[func() []ModuleState]

	mu       sync.Mutex
	reported []sdk.Event // waiting for the next tick, at most reportCap
}

// reportCap bounds the reported events waiting while the module is not running.
const reportCap = 256

// Default is the probe the registered `internal` kind reads.
var Default = NewProbe()

// NewProbe returns a probe with nothing set.
func NewProbe() *Probe { return &Probe{} }

// RecordFrame adds one frame's duration and why it was drawn; it is lock-free and
// allocation-free for the render loop.
func (p *Probe) RecordFrame(d time.Duration, why FrameReason) {
	p.frames.record(int64(d))
	p.why[why].Add(1)
}

// Frames is how many frames have been drawn for why.
func (p *Probe) Frames(why FrameReason) uint64 { return p.why[why].Load() }

// FrameReason is why the app drew a frame.
type FrameReason uint8

// The reasons, in the order the app checks them.
const (
	FrameInput     FrameReason = iota // a key, the pointer, the window, or a reply to them
	FrameAnimation                    // something moving
	FrameWorld                        // a module changed the world
	FrameSeries                       // series or events arrived
	FrameLive                         // a live time bar moved on
	FrameDesktop                      // the desktop's look changed
	FrameReasons                      // how many there are
)

var reasonNames = [FrameReasons]string{"input", "animation", "world", "series", "live", "desktop"}

func (r FrameReason) String() string { return reasonNames[r] }

// BusTraffic is the app's event bus: its topics, and the messages published and dropped
// across them since it started.
type BusTraffic struct {
	Topics             int
	Published, Dropped uint64
}

// LogSource hands out the log lines written since seq, and the sequence number to ask from next.
type LogSource interface {
	Since(seq uint64) (lines []string, next uint64)
}

// SetBus sets how to read the bus's traffic.
func (p *Probe) SetBus(fn func() BusTraffic) { p.bus.Store(&fn) }

// SetLog sets the log whose lines become events.
func (p *Probe) SetLog(l LogSource) { p.log.Store(&l) }

// Report adds e, such as an action's outcome on another module's entity, to the next tick.
// The module sets its ID and source, and its time if it is zero.
func (p *Probe) Report(e sdk.Event) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.reported) < reportCap {
		p.reported = append(p.reported, e)
	}
}

func (p *Probe) takeReported() []sdk.Event {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := p.reported
	p.reported = nil
	return out
}

func (p *Probe) busTraffic() (BusTraffic, bool) {
	if fn := p.bus.Load(); fn != nil {
		return (*fn)(), true
	}
	return BusTraffic{}, false
}

// SetModules sets how to list the module instances and their freshness.
func (p *Probe) SetModules(fn func() []ModuleState) { p.modules.Store(&fn) }

func (p *Probe) moduleStates() []ModuleState {
	if fn := p.modules.Load(); fn != nil {
		return (*fn)()
	}
	return nil
}

// frameStats accumulates frame durations between samples.
type frameStats struct {
	count, sum, max atomic.Int64
}

func (f *frameStats) record(ns int64) {
	f.count.Add(1)
	f.sum.Add(ns)
	for m := f.max.Load(); ns > m; m = f.max.Load() {
		if f.max.CompareAndSwap(m, ns) {
			return
		}
	}
}

// take returns and resets the frames since the last take; a frame recorded mid-take may skew
// one sample slightly, which a 1 s mean tolerates.
func (f *frameStats) take() (count int64, mean, maxDur time.Duration) {
	n, s, m := f.count.Swap(0), f.sum.Swap(0), f.max.Swap(0)
	if n == 0 {
		return 0, 0, 0
	}
	return n, time.Duration(s / n), time.Duration(m)
}
