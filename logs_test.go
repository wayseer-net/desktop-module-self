package self

import (
	"testing"
	"time"
	"wayseer/pkg/sdk"
)

func TestParseLogLine(t *testing.T) {
	at, sev, msg := parseLogLine(`time=2026-09-25T10:00:00.123+02:00 level=WARN msg="slow frame" ms=40`)
	if want := time.Date(2026, 9, 25, 8, 0, 0, 123e6, time.UTC); !at.Equal(want) {
		t.Errorf("time %v, want %v", at, want)
	}
	if sev != sdk.SevWarn || msg != "slow frame ms=40" {
		t.Errorf("severity %v, message %q", sev, msg)
	}
}

func TestParseLogLineKeepsUnrecognisedLinesWhole(t *testing.T) {
	at, sev, msg := parseLogLine("plain text")
	if !at.IsZero() || sev != sdk.SevInfo || msg != "plain text" {
		t.Errorf("%v %v %q", at, sev, msg)
	}
}

func TestParseLogLineSeverities(t *testing.T) {
	for level, want := range map[string]sdk.Severity{
		"DEBUG": sdk.SevDebug, "INFO": sdk.SevInfo, "INFO+2": sdk.SevInfo,
		"WARN": sdk.SevWarn, "ERROR": sdk.SevError, "ERROR+4": sdk.SevCritical,
	} {
		if _, got, _ := parseLogLine("time=2026-09-25T10:00:00Z level=" + level + " msg=x"); got != want {
			t.Errorf("%s → %v, want %v", level, got, want)
		}
	}
}
