package self

import (
	"log/slog"
	"strconv"
	"strings"
	"time"

	"wayseer.dev/sdk"
)

// parseLogLine reads a slog text line; a line in another shape is an info message dated zero.
func parseLogLine(line string) (at time.Time, sev sdk.Severity, msg string) {
	rest, ok := strings.CutPrefix(line, "time=")
	if !ok {
		return time.Time{}, sdk.SevInfo, line
	}
	ts, rest, _ := strings.Cut(rest, " ")
	lv, rest, _ := strings.Cut(strings.TrimPrefix(rest, "level="), " ")
	at, err := time.Parse(time.RFC3339Nano, ts)
	var level slog.Level
	if err != nil || level.UnmarshalText([]byte(lv)) != nil {
		return time.Time{}, sdk.SevInfo, line
	}
	return at.UTC(), severity(level), message(strings.TrimPrefix(rest, "msg="))
}

func severity(l slog.Level) sdk.Severity {
	switch {
	case l < slog.LevelInfo:
		return sdk.SevDebug
	case l < slog.LevelWarn:
		return sdk.SevInfo
	case l < slog.LevelError:
		return sdk.SevWarn
	case l < slog.LevelError+4:
		return sdk.SevError
	}
	return sdk.SevCritical
}

// message unquotes the msg value and keeps the attributes after it as they are.
func message(s string) string {
	if !strings.HasPrefix(s, `"`) {
		return s
	}
	q, err := strconv.QuotedPrefix(s)
	if err != nil {
		return s
	}
	text, _ := strconv.Unquote(q)
	return strings.TrimSpace(text + s[len(q):])
}
