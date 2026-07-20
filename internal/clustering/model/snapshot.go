package model

import "time"

const PeerStoreVersion = 1

type Snapshot struct {
	Version   int       `json:"version"`
	UpdatedAt time.Time `json:"updated_at"`
	Peers     []Peer    `json:"peers"`
}
