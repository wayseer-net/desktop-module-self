package self

import (
	"mindseye/internal/model"
	"testing"
	"time"
)

func TestParseLogLine(t *testing.T) {
	at, sev, msg := parseLogLine(`time=2026-09-25T10:00:00.123+02:00 level=WARN msg="slow frame" ms=40`)
	if want := time.Date(2026, 9, 25, 8, 0, 0, 123e6, time.UTC); !at.Equal(want) {
		t.Errorf("time %v, want %v", at, want)
	}
	if sev != model.SevWarn || msg != "slow frame ms=40" {
		t.Errorf("severity %v, message %q", sev, msg)
	}
}

func TestParseLogLineKeepsUnrecognisedLinesWhole(t *testing.T) {
	at, sev, msg := parseLogLine("plain text")
	if !at.IsZero() || sev != model.SevInfo || msg != "plain text" {
		t.Errorf("%v %v %q", at, sev, msg)
	}
}

func TestParseLogLineSeverities(t *testing.T) {
	for level, want := range map[string]model.Severity{
		"DEBUG": model.SevDebug, "INFO": model.SevInfo, "INFO+2": model.SevInfo,
		"WARN": model.SevWarn, "ERROR": model.SevError, "ERROR+4": model.SevCritical,
	} {
		if _, got, _ := parseLogLine("time=2026-09-25T10:00:00Z level=" + level + " msg=x"); got != want {
			t.Errorf("%s → %v, want %v", level, got, want)
		}
	}
}
