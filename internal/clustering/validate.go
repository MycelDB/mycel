package clustering

import (
	"fmt"
	"net"
	"strconv"
	"strings"
)

func ValidateIdentity(id NodeIdentity) error {
	if id.Version != NodeIdentityVersion {
		return fmt.Errorf("unsupported clustering node identity version %d", id.Version)
	}
	if strings.TrimSpace(id.NodeID) == "" {
		return fmt.Errorf("clustering node_id is required")
	}
	if strings.TrimSpace(id.ClusterID) == "" && (id.ClusterAdmitted || id.ClusterBootstrap) {
		return fmt.Errorf("clustering cluster_id is required for admitted nodes")
	}
	if id.CreatedAt.IsZero() {
		return fmt.Errorf("clustering created_at is required")
	}
	if id.UpdatedAt.IsZero() {
		return fmt.Errorf("clustering updated_at is required")
	}
	return ValidateBackendAdvertiseAddr(id.BackendAdvertiseAddr)
}

func ValidateBackendAdvertiseAddr(addr string) error {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return nil
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("MYCELD_CLUSTER_BACKEND_ADVERTISE_ADDR must be host:port: %w", err)
	}
	if strings.TrimSpace(host) == "" {
		return fmt.Errorf("MYCELD_CLUSTER_BACKEND_ADVERTISE_ADDR host must not be empty")
	}
	if strings.TrimSpace(port) == "" {
		return fmt.Errorf("MYCELD_CLUSTER_BACKEND_ADVERTISE_ADDR port must not be empty")
	}
	p, err := strconv.Atoi(port)
	if err != nil || p <= 0 || p > 65535 {
		return fmt.Errorf("MYCELD_CLUSTER_BACKEND_ADVERTISE_ADDR port must be valid")
	}
	trimmedHost := strings.Trim(host, "[]")
	if trimmedHost == "0.0.0.0" || trimmedHost == "::" {
		return fmt.Errorf("MYCELD_CLUSTER_BACKEND_ADVERTISE_ADDR must not use wildcard host")
	}
	return nil
}
