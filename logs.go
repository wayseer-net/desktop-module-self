package self

import (
	"log/slog"
	"mindseye/internal/model"
	"strconv"
	"strings"
	"time"
)

// parseLogLine reads a slog text line; a line in another shape is an info message dated zero.
func parseLogLine(line string) (at time.Time, sev model.Severity, msg string) {
	rest, ok := strings.CutPrefix(line, "time=")
	if !ok {
		return time.Time{}, model.SevInfo, line
	}
	ts, rest, _ := strings.Cut(rest, " ")
	lv, rest, _ := strings.Cut(strings.TrimPrefix(rest, "level="), " ")
	at, err := time.Parse(time.RFC3339Nano, ts)
	var level slog.Level
	if err != nil || level.UnmarshalText([]byte(lv)) != nil {
		return time.Time{}, model.SevInfo, line
	}
	return at.UTC(), severity(level), message(strings.TrimPrefix(rest, "msg="))
}

func severity(l slog.Level) model.Severity {
	switch {
	case l < slog.LevelInfo:
		return model.SevDebug
	case l < slog.LevelWarn:
		return model.SevInfo
	case l < slog.LevelError:
		return model.SevWarn
	case l < slog.LevelError+4:
		return model.SevError
	}
	return model.SevCritical
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
