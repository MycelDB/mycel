package routing

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/myceldb/mycel/internal/clustering/consensus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Reason describes a Phase E routing decision or failure cause. Keep these
// values stable enough for logs/diagnostics; they are not public API enums yet.
type Reason string

const (
	ReasonLocal          Reason = "local"
	ReasonForwarded      Reason = "forwarded"
	ReasonUnavailable    Reason = "unavailable"
	ReasonUnknownHome    Reason = "unknown_home"
	ReasonHomeMismatch   Reason = "home_mismatch"
	ReasonForwardingLoop Reason = "forwarding_loop"
)

var (
	ErrRouteUnavailable    = errors.New("route unavailable")
	ErrUnknownSessionHome  = errors.New("unknown session home")
	ErrSessionHomeMismatch = errors.New("session home mismatch")
	ErrForwardingLoop      = errors.New("forwarding loop")
)

// Decision captures a route decision for a session/transaction/partition keyed
// request. Phase E uses this for diagnostics before the actual forwarding
// substrate is introduced.
type Decision struct {
	Reason      Reason
	HomeNode    consensus.NodeID
	TargetNode  consensus.NodeID
	LocalNode   consensus.NodeID
	BackendAddr string
	Generation  uint64
}

func (d Decision) IsLocal() bool {
	return d.Reason == ReasonLocal || (d.TargetNode != 0 && d.TargetNode == d.LocalNode)
}
func (d Decision) IsForwarded() bool {
	return d.Reason == ReasonForwarded || (d.TargetNode != 0 && d.LocalNode != 0 && d.TargetNode != d.LocalNode)
}

// RouteError is the canonical internal error for Phase E routing failures. It
// implements gRPC status conversion so API and backend adapters map failures
// consistently.
type RouteError struct {
	Reason      Reason
	Message     string
	HomeNode    consensus.NodeID
	TargetNode  consensus.NodeID
	LocalNode   consensus.NodeID
	BackendAddr string
	Cause       error
}

func (e *RouteError) Error() string {
	if e == nil {
		return "<nil>"
	}
	msg := strings.TrimSpace(e.Message)
	if msg == "" {
		msg = string(e.Reason)
	}
	ctx := []string{}
	if e.LocalNode != 0 {
		ctx = append(ctx, fmt.Sprintf("local_node=%d", e.LocalNode))
	}
	if e.HomeNode != 0 {
		ctx = append(ctx, fmt.Sprintf("home_node=%d", e.HomeNode))
	}
	if e.TargetNode != 0 {
		ctx = append(ctx, fmt.Sprintf("target_node=%d", e.TargetNode))
	}
	if strings.TrimSpace(e.BackendAddr) != "" {
		ctx = append(ctx, fmt.Sprintf("backend_addr=%q", strings.TrimSpace(e.BackendAddr)))
	}
	if len(ctx) > 0 {
		msg += " (" + strings.Join(ctx, " ") + ")"
	}
	if e.Cause != nil {
		msg += ": " + e.Cause.Error()
	}
	return msg
}

func (e *RouteError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func (e *RouteError) Is(target error) bool {
	if e == nil {
		return false
	}
	switch target {
	case ErrRouteUnavailable:
		return e.Reason == ReasonUnavailable
	case ErrUnknownSessionHome:
		return e.Reason == ReasonUnknownHome
	case ErrSessionHomeMismatch:
		return e.Reason == ReasonHomeMismatch
	case ErrForwardingLoop:
		return e.Reason == ReasonForwardingLoop
	default:
		return false
	}
}

func (e *RouteError) GRPCStatus() *status.Status {
	if e == nil {
		return status.New(codes.OK, "")
	}
	code := codes.Unavailable
	switch e.Reason {
	case ReasonUnknownHome:
		code = codes.NotFound
	case ReasonHomeMismatch:
		code = codes.FailedPrecondition
	case ReasonForwardingLoop:
		code = codes.FailedPrecondition
	case ReasonUnavailable:
		code = codes.Unavailable
	}
	return status.New(code, e.Error())
}

func NewRouteUnavailable(message string, opts ...func(*RouteError)) *RouteError {
	return newRouteError(ReasonUnavailable, message, opts...)
}
func NewUnknownSessionHome(message string, opts ...func(*RouteError)) *RouteError {
	return newRouteError(ReasonUnknownHome, message, opts...)
}
func NewSessionHomeMismatch(message string, opts ...func(*RouteError)) *RouteError {
	return newRouteError(ReasonHomeMismatch, message, opts...)
}
func NewForwardingLoop(message string, opts ...func(*RouteError)) *RouteError {
	return newRouteError(ReasonForwardingLoop, message, opts...)
}

func newRouteError(reason Reason, message string, opts ...func(*RouteError)) *RouteError {
	err := &RouteError{Reason: reason, Message: message}
	for _, opt := range opts {
		if opt != nil {
			opt(err)
		}
	}
	return err
}

func WithHomeNode(node consensus.NodeID) func(*RouteError) {
	return func(e *RouteError) { e.HomeNode = node }
}
func WithTargetNode(node consensus.NodeID) func(*RouteError) {
	return func(e *RouteError) { e.TargetNode = node }
}
func WithLocalNode(node consensus.NodeID) func(*RouteError) {
	return func(e *RouteError) { e.LocalNode = node }
}
func WithBackendAddr(addr string) func(*RouteError) {
	return func(e *RouteError) { e.BackendAddr = strings.TrimSpace(addr) }
}
func WithCause(cause error) func(*RouteError) { return func(e *RouteError) { e.Cause = cause } }

const (
	ForwardedFromMetadataKey = "x-mycel-forwarded-from"
	RouteDepthMetadataKey    = "x-mycel-route-depth"
	DefaultMaxRouteDepth     = 1
)

// ForwardingGuard prevents backend forwarding loops. V1 only needs one hop:
// public ingress node -> home/leader node. If a forwarded request attempts to
// forward again, it fails closed.
type ForwardingGuard struct {
	LocalNode consensus.NodeID
	MaxDepth  int
}

func (g ForwardingGuard) maxDepth() int {
	if g.MaxDepth <= 0 {
		return DefaultMaxRouteDepth
	}
	return g.MaxDepth
}

func (g ForwardingGuard) IncomingDepth(ctx context.Context) int {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return 0
	}
	return parseRouteDepth(md.Get(RouteDepthMetadataKey))
}

func (g ForwardingGuard) Check(ctx context.Context) error {
	depth := g.IncomingDepth(ctx)
	if depth > g.maxDepth() {
		return NewForwardingLoop("route depth exceeds maximum", WithLocalNode(g.LocalNode))
	}
	if depth == g.maxDepth() {
		return NewForwardingLoop("request has already been forwarded", WithLocalNode(g.LocalNode))
	}
	return nil
}

func (g ForwardingGuard) OutgoingContext(ctx context.Context) (context.Context, error) {
	if err := g.Check(ctx); err != nil {
		return ctx, err
	}
	depth := g.IncomingDepth(ctx) + 1
	pairs := []string{RouteDepthMetadataKey, strconv.Itoa(depth)}
	if g.LocalNode != 0 {
		pairs = append(pairs, ForwardedFromMetadataKey, fmt.Sprintf("%d", g.LocalNode))
	}
	return metadata.AppendToOutgoingContext(ctx, pairs...), nil
}

func parseRouteDepth(values []string) int {
	maxDepth := 0
	for _, value := range values {
		parsed, err := strconv.Atoi(strings.TrimSpace(value))
		if err == nil && parsed > maxDepth {
			maxDepth = parsed
		}
	}
	return maxDepth
}
