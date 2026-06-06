package query

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"martinbeauvais.com/mbgit/knotbase/knotdb/domain/graph"
)

const dateLayout = "2006-01-02"

// Expr is a boolean expression evaluated against a query row.
type Expr interface {
	eval(row evalRow) (bool, error)
}

// ValueExpr is an expression that produces a comparable value.
type ValueExpr interface {
	evalValue(row evalRow) (any, error)
}

type evalRow struct {
	bindings map[string][]any
}

// Prop references a node property by bound alias and property name.
func Prop(alias, name string) PropExpr { return PropExpr{Alias: alias, Name: name} }

// PropExpr references a node property by bound alias and property name.
type PropExpr struct {
	Alias string
	Name  string
}

func (p PropExpr) evalValue(row evalRow) (any, error) {
	values := row.bindings[p.Alias]
	if len(values) == 0 {
		return nil, fmt.Errorf("alias %q is not bound", p.Alias)
	}
	n, ok := values[0].(graph.Node)
	if !ok {
		return nil, fmt.Errorf("alias %q is not a node", p.Alias)
	}
	return n.Props[p.Name], nil
}

// Between checks that value is inclusively between low and high.
func Between(value, low, high ValueExpr) Expr { return betweenExpr{value: value, low: low, high: high} }

type betweenExpr struct {
	value ValueExpr
	low   ValueExpr
	high  ValueExpr
}

func (b betweenExpr) eval(row evalRow) (bool, error) {
	value, err := b.value.evalValue(row)
	if err != nil {
		return false, err
	}
	low, err := b.low.evalValue(row)
	if err != nil {
		return false, err
	}
	high, err := b.high.evalValue(row)
	if err != nil {
		return false, err
	}
	cLow, err := compareValues(value, low)
	if err != nil {
		return false, err
	}
	cHigh, err := compareValues(value, high)
	if err != nil {
		return false, err
	}
	return cLow >= 0 && cHigh <= 0, nil
}

// And requires all expressions to be true.
func And(exprs ...Expr) Expr { return andExpr(exprs) }

type andExpr []Expr

func (a andExpr) eval(row evalRow) (bool, error) {
	for _, expr := range a {
		ok, err := expr.eval(row)
		if err != nil || !ok {
			return ok, err
		}
	}
	return true, nil
}

// Duration is a calendar duration used by date expressions.
type Duration struct{ days int }

// Days creates a calendar-day duration.
func Days(n int) Duration { return Duration{days: n} }

// CurrentDate returns the current local calendar date at execution time.
func CurrentDate() DateExpr { return DateExpr{current: true} }

// Date creates a literal date expression from a YYYY-MM-DD string.
func Date(value string) DateExpr { return DateExpr{literal: value} }

// DateExpr evaluates to a calendar date.
type DateExpr struct {
	current bool
	literal string
	offset  int
}

// Minus subtracts a calendar duration from the date expression.
func (d DateExpr) Minus(duration Duration) DateExpr {
	d.offset -= duration.days
	return d
}

func (d DateExpr) evalValue(row evalRow) (any, error) {
	var t time.Time
	if d.current {
		now := time.Now()
		t = time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.Local)
	} else {
		parsed, err := time.ParseInLocation(dateLayout, d.literal, time.Local)
		if err != nil {
			return nil, err
		}
		t = parsed
	}
	return t.AddDate(0, 0, d.offset), nil
}

func compareValues(left, right any) (int, error) {
	if lt, ok := asDate(left); ok {
		rt, ok := asDate(right)
		if !ok {
			return 0, fmt.Errorf("cannot compare date to %T", right)
		}
		return compareTime(lt, rt), nil
	}
	if rt, ok := asDate(right); ok {
		lt, ok := asDate(left)
		if !ok {
			return 0, fmt.Errorf("cannot compare %T to date", left)
		}
		return compareTime(lt, rt), nil
	}
	if lf, ok := asFloat(left); ok {
		rf, ok := asFloat(right)
		if !ok {
			return 0, fmt.Errorf("cannot compare number to %T", right)
		}
		if lf < rf {
			return -1, nil
		}
		if lf > rf {
			return 1, nil
		}
		return 0, nil
	}
	ls, lok := left.(string)
	rs, rok := right.(string)
	if lok && rok {
		return strings.Compare(ls, rs), nil
	}
	return 0, fmt.Errorf("unsupported comparison between %T and %T", left, right)
}

func compareTime(left, right time.Time) int {
	if left.Before(right) {
		return -1
	}
	if left.After(right) {
		return 1
	}
	return 0
}

func asDate(value any) (time.Time, bool) {
	switch v := value.(type) {
	case time.Time:
		return time.Date(v.Year(), v.Month(), v.Day(), 0, 0, 0, 0, time.Local), true
	case string:
		t, err := time.ParseInLocation(dateLayout, v, time.Local)
		if err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

func asFloat(value any) (float64, bool) {
	switch v := value.(type) {
	case int:
		return float64(v), true
	case int8:
		return float64(v), true
	case int16:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint:
		return float64(v), true
	case uint8:
		return float64(v), true
	case uint16:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint64:
		return float64(v), true
	case float32:
		return float64(v), true
	case float64:
		return v, true
	case json.Number:
		f, err := v.Float64()
		return f, err == nil
	}
	return 0, false
}
