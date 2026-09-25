// Package self is the `internal` module: Mind's Eye describing itself. The app feeds a Probe
// (frame times, bus, log ring, module states) and the module turns it into entities, series
// and log events, giving tests and the demo a source that always exists. It lives outside a
// directory named internal so cmd/mindseye can import it.
package self
