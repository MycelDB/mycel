package routing

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/consensus"
)

const (
	SessionIDPrefix     = "s"
	TransactionIDPrefix = "tx"
)

func NewSessionID(homeNode consensus.NodeID) string {
	return newRoutedID(SessionIDPrefix, homeNode)
}

func NewTransactionID(homeNode consensus.NodeID) string {
	return newRoutedID(TransactionIDPrefix, homeNode)
}

func ParseSessionHomeNode(sessionID string) (consensus.NodeID, bool, error) {
	return parseHomeNode(SessionIDPrefix, sessionID)
}

func ParseTransactionHomeNode(transactionID string) (consensus.NodeID, bool, error) {
	return parseHomeNode(TransactionIDPrefix, transactionID)
}

func newRoutedID(prefix string, homeNode consensus.NodeID) string {
	id := uuid.NewString()
	if homeNode == 0 {
		return id
	}
	return fmt.Sprintf("%s.%d.%s", prefix, homeNode, id)
}

func parseHomeNode(prefix string, id string) (consensus.NodeID, bool, error) {
	trimmed := strings.TrimSpace(id)
	parts := strings.Split(trimmed, ".")
	if len(parts) == 1 {
		return 0, false, nil
	}
	if len(parts) != 3 || parts[0] != prefix {
		return 0, false, fmt.Errorf("invalid routed %s id", prefix)
	}
	node, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil || node == 0 {
		return 0, false, fmt.Errorf("invalid routed %s home node", prefix)
	}
	if _, err := uuid.Parse(parts[2]); err != nil {
		return 0, false, fmt.Errorf("invalid routed %s uuid: %w", prefix, err)
	}
	return consensus.NodeID(node), true, nil
}
