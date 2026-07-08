package backup

import (
	"testing"
	"time"
)

func TestNextRunIntervalWithoutLastSuccess(t *testing.T) {
	now := mustTime(t, "2026-01-02T10:00:00Z")
	next, err := NextRun(Policy{ScheduleKind: ScheduleKindInterval, Interval: 6 * time.Hour}, now, time.Time{})
	if err != nil {
		t.Fatalf("NextRun() error = %v", err)
	}
	want := mustTime(t, "2026-01-02T16:00:00Z")
	if !next.Equal(want) {
		t.Fatalf("next = %s, want %s", next, want)
	}
}

func TestNextRunIntervalUsesLastSuccess(t *testing.T) {
	now := mustTime(t, "2026-01-02T10:00:00Z")
	last := mustTime(t, "2026-01-02T09:00:00Z")
	next, err := NextRun(Policy{ScheduleKind: ScheduleKindInterval, Interval: 6 * time.Hour}, now, last)
	if err != nil {
		t.Fatalf("NextRun() error = %v", err)
	}
	want := mustTime(t, "2026-01-02T15:00:00Z")
	if !next.Equal(want) {
		t.Fatalf("next = %s, want %s", next, want)
	}
}

func TestNextRunDailyBeforeAndAfterTime(t *testing.T) {
	policy := Policy{ScheduleKind: ScheduleKindDaily, TimeOfDay: "22:00", Timezone: "UTC"}
	before := mustTime(t, "2026-01-02T10:00:00Z")
	next, err := NextRun(policy, before, time.Time{})
	if err != nil {
		t.Fatalf("NextRun(before) error = %v", err)
	}
	want := mustTime(t, "2026-01-02T22:00:00Z")
	if !next.Equal(want) {
		t.Fatalf("before next = %s, want %s", next, want)
	}
	after := mustTime(t, "2026-01-02T23:00:00Z")
	next, err = NextRun(policy, after, time.Time{})
	if err != nil {
		t.Fatalf("NextRun(after) error = %v", err)
	}
	want = mustTime(t, "2026-01-03T22:00:00Z")
	if !next.Equal(want) {
		t.Fatalf("after next = %s, want %s", next, want)
	}
}

func TestNextRunDailyUsesTimezone(t *testing.T) {
	policy := Policy{ScheduleKind: ScheduleKindDaily, TimeOfDay: "22:00", Timezone: "America/Toronto"}
	now := mustTime(t, "2026-01-02T20:00:00Z") // 15:00 Toronto in January.
	next, err := NextRun(policy, now, time.Time{})
	if err != nil {
		t.Fatalf("NextRun() error = %v", err)
	}
	want := mustTime(t, "2026-01-03T03:00:00Z") // 22:00 America/Toronto.
	if !next.Equal(want) {
		t.Fatalf("next = %s, want %s", next, want)
	}
}

func TestNextRunManualSuccessDoesNotShiftDailySchedule(t *testing.T) {
	policy := Policy{ScheduleKind: ScheduleKindDaily, TimeOfDay: "22:00", Timezone: "UTC"}
	now := mustTime(t, "2026-01-02T10:00:00Z")
	manualSuccess := mustTime(t, "2026-01-02T10:00:00Z")
	next, err := NextRun(policy, now, manualSuccess)
	if err != nil {
		t.Fatalf("NextRun() error = %v", err)
	}
	want := mustTime(t, "2026-01-02T22:00:00Z")
	if !next.Equal(want) {
		t.Fatalf("next = %s, want %s", next, want)
	}
}

func TestNextRunWeeklySameDayBeforeTime(t *testing.T) {
	policy := Policy{ScheduleKind: ScheduleKindWeekly, TimeOfDay: "02:00", Timezone: "UTC", Weekdays: []int{0}}
	now := mustTime(t, "2026-01-04T01:00:00Z") // Sunday.
	next, err := NextRun(policy, now, time.Time{})
	if err != nil {
		t.Fatalf("NextRun() error = %v", err)
	}
	want := mustTime(t, "2026-01-04T02:00:00Z")
	if !next.Equal(want) {
		t.Fatalf("next = %s, want %s", next, want)
	}
}

func TestNextRunWeeklyAfterTimeUsesNextConfiguredDay(t *testing.T) {
	policy := Policy{ScheduleKind: ScheduleKindWeekly, TimeOfDay: "02:00", Timezone: "UTC", Weekdays: []int{0, 3}}
	now := mustTime(t, "2026-01-04T03:00:00Z") // Sunday after slot.
	next, err := NextRun(policy, now, time.Time{})
	if err != nil {
		t.Fatalf("NextRun() error = %v", err)
	}
	want := mustTime(t, "2026-01-07T02:00:00Z") // Wednesday.
	if !next.Equal(want) {
		t.Fatalf("next = %s, want %s", next, want)
	}
}

func TestNextRunCalendarRunMissed(t *testing.T) {
	policy := Policy{ScheduleKind: ScheduleKindDaily, TimeOfDay: "22:00", Timezone: "UTC", RunMissed: true}
	now := mustTime(t, "2026-01-03T10:00:00Z")
	lastSuccess := mustTime(t, "2026-01-02T20:00:00Z")
	next, err := NextRun(policy, now, lastSuccess)
	if err != nil {
		t.Fatalf("NextRun() error = %v", err)
	}
	if !next.Equal(now) {
		t.Fatalf("missed next = %s, want now %s", next, now)
	}
}

func TestValidateScheduleRejectsInvalidValues(t *testing.T) {
	tests := []Policy{
		{ScheduleKind: "hourly"},
		{ScheduleKind: ScheduleKindDaily, TimeOfDay: "9:00", Timezone: "UTC"},
		{ScheduleKind: ScheduleKindDaily, TimeOfDay: "09:00", Timezone: "Mars/Olympus"},
		{ScheduleKind: ScheduleKindWeekly, TimeOfDay: "09:00", Timezone: "UTC"},
		{ScheduleKind: ScheduleKindWeekly, TimeOfDay: "09:00", Timezone: "UTC", Weekdays: []int{7}},
	}
	for _, tt := range tests {
		if err := ValidateSchedule(tt); err == nil {
			t.Fatalf("ValidateSchedule(%#v) succeeded, want error", tt)
		}
	}
}

func mustTime(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		t.Fatalf("parse time %q: %v", value, err)
	}
	return parsed
}
