package backup

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// NextRun returns the next scheduled run time for policy at now. Interval
// schedules are based on lastSuccess when available. Calendar schedules are
// based on wall-clock slots in the configured timezone and are not shifted by
// manual backup success.
func NextRun(policy Policy, now time.Time, lastSuccess time.Time) (time.Time, error) {
	policy = EffectivePolicy("", policy)
	now = now.UTC()
	lastSuccess = lastSuccess.UTC()
	switch policy.ScheduleKind {
	case "", ScheduleKindInterval:
		return nextIntervalRun(policy, now, lastSuccess), nil
	case ScheduleKindDaily:
		return nextDailyRun(policy, now, lastSuccess)
	case ScheduleKindWeekly:
		return nextWeeklyRun(policy, now, lastSuccess)
	default:
		return time.Time{}, fmt.Errorf("unsupported backup schedule_kind %q", policy.ScheduleKind)
	}
}

func ValidateSchedule(policy Policy) error {
	policy = EffectivePolicy("", policy)
	switch policy.ScheduleKind {
	case ScheduleKindInterval:
		if policy.Interval <= 0 {
			return fmt.Errorf("interval must be positive")
		}
		return nil
	case ScheduleKindDaily:
		if _, err := parseTimeOfDay(policy.TimeOfDay); err != nil {
			return err
		}
		_, err := time.LoadLocation(policy.Timezone)
		return err
	case ScheduleKindWeekly:
		if _, err := parseTimeOfDay(policy.TimeOfDay); err != nil {
			return err
		}
		if _, err := time.LoadLocation(policy.Timezone); err != nil {
			return err
		}
		if len(policy.Weekdays) == 0 {
			return fmt.Errorf("weekdays is required for weekly backup schedule")
		}
		for _, weekday := range policy.Weekdays {
			if weekday < 0 || weekday > 6 {
				return fmt.Errorf("weekday %d out of range", weekday)
			}
		}
		return nil
	default:
		return fmt.Errorf("unsupported backup schedule_kind %q", policy.ScheduleKind)
	}
}

func nextIntervalRun(policy Policy, now time.Time, lastSuccess time.Time) time.Time {
	base := now
	if !lastSuccess.IsZero() {
		base = lastSuccess
	}
	next := base.Add(policy.Interval)
	if policy.RunMissed && !lastSuccess.IsZero() && !next.After(now) {
		return now
	}
	return next
}

func nextDailyRun(policy Policy, now time.Time, lastSuccess time.Time) (time.Time, error) {
	slot, previous, err := nextAndPreviousDailySlot(policy, now)
	if err != nil {
		return time.Time{}, err
	}
	if policy.RunMissed && missedSlot(previous, now, lastSuccess) {
		return now, nil
	}
	return slot, nil
}

func nextWeeklyRun(policy Policy, now time.Time, lastSuccess time.Time) (time.Time, error) {
	slot, previous, err := nextAndPreviousWeeklySlot(policy, now)
	if err != nil {
		return time.Time{}, err
	}
	if policy.RunMissed && missedSlot(previous, now, lastSuccess) {
		return now, nil
	}
	return slot, nil
}

func missedSlot(previous time.Time, now time.Time, lastSuccess time.Time) bool {
	return !previous.IsZero() && !lastSuccess.IsZero() && !previous.After(now) && lastSuccess.Before(previous)
}

func nextAndPreviousDailySlot(policy Policy, now time.Time) (time.Time, time.Time, error) {
	hm, err := parseTimeOfDay(policy.TimeOfDay)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	loc, err := time.LoadLocation(policy.Timezone)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	localNow := now.In(loc)
	today := time.Date(localNow.Year(), localNow.Month(), localNow.Day(), hm.hour, hm.minute, 0, 0, loc)
	if localNow.Before(today) {
		return today.UTC(), today.AddDate(0, 0, -1).UTC(), nil
	}
	return today.AddDate(0, 0, 1).UTC(), today.UTC(), nil
}

func nextAndPreviousWeeklySlot(policy Policy, now time.Time) (time.Time, time.Time, error) {
	hm, err := parseTimeOfDay(policy.TimeOfDay)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	loc, err := time.LoadLocation(policy.Timezone)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	weekdays := normalizedWeekdays(policy.Weekdays)
	if len(weekdays) == 0 {
		return time.Time{}, time.Time{}, fmt.Errorf("weekdays is required for weekly backup schedule")
	}
	localNow := now.In(loc)
	var next time.Time
	for dayOffset := 0; dayOffset <= 7; dayOffset++ {
		candidateDay := localNow.AddDate(0, 0, dayOffset)
		weekday := int(candidateDay.Weekday())
		if !containsWeekday(weekdays, weekday) {
			continue
		}
		candidate := time.Date(candidateDay.Year(), candidateDay.Month(), candidateDay.Day(), hm.hour, hm.minute, 0, 0, loc)
		if localNow.Before(candidate) {
			next = candidate
			break
		}
	}
	if next.IsZero() {
		return time.Time{}, time.Time{}, fmt.Errorf("could not compute next weekly backup run")
	}
	var previous time.Time
	for dayOffset := 0; dayOffset >= -7; dayOffset-- {
		candidateDay := localNow.AddDate(0, 0, dayOffset)
		weekday := int(candidateDay.Weekday())
		if !containsWeekday(weekdays, weekday) {
			continue
		}
		candidate := time.Date(candidateDay.Year(), candidateDay.Month(), candidateDay.Day(), hm.hour, hm.minute, 0, 0, loc)
		if !candidate.After(localNow) {
			previous = candidate
			break
		}
	}
	return next.UTC(), previous.UTC(), nil
}

func normalizedWeekdays(values []int) []int {
	seen := map[int]bool{}
	out := []int{}
	for _, value := range values {
		if value < 0 || value > 6 || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	sort.Ints(out)
	return out
}

func containsWeekday(values []int, weekday int) bool {
	for _, value := range values {
		if value == weekday {
			return true
		}
	}
	return false
}

type hourMinute struct {
	hour   int
	minute int
}

func parseTimeOfDay(value string) (hourMinute, error) {
	value = strings.TrimSpace(value)
	parts := strings.Split(value, ":")
	if len(parts) != 2 || len(parts[0]) != 2 || len(parts[1]) != 2 {
		return hourMinute{}, fmt.Errorf("time_of_day must use HH:MM 24-hour format")
	}
	hour, err := strconv.Atoi(parts[0])
	if err != nil || hour < 0 || hour > 23 {
		return hourMinute{}, fmt.Errorf("time_of_day hour out of range")
	}
	minute, err := strconv.Atoi(parts[1])
	if err != nil || minute < 0 || minute > 59 {
		return hourMinute{}, fmt.Errorf("time_of_day minute out of range")
	}
	return hourMinute{hour: hour, minute: minute}, nil
}
