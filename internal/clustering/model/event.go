package model

import "time"

type EventType string

const (
	EventPeerAdded        EventType = "peer_added"
	EventPeerUpdated      EventType = "peer_updated"
	EventPeerRemoved      EventType = "peer_removed"
	EventPeerStateChanged EventType = "peer_state_changed"
	EventSelfUpdated      EventType = "self_updated"
	EventSnapshotMerged   EventType = "snapshot_merged"
)

type Event struct {
	Type     EventType `json:"type"`
	Peer     Peer      `json:"peer"`
	Previous *Peer     `json:"previous,omitempty"`
	At       time.Time `json:"at"`
}
