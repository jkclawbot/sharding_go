// Package health provides health checking for cluster nodes
package health

import (
	"context"
	"net"
	"net/http"
	"sync"
	"time"
)

// DefaultTimeout is the default health check timeout
const DefaultTimeout = 5 * time.Second

// Checker defines the interface for health checks
type Checker interface {
	Check(ctx context.Context, addr string) error
}

// HTTPChecker checks if an HTTP endpoint is healthy
type HTTPChecker struct {
	Timeout time.Duration
	Path    string
}

// NewHTTPChecker creates a new HTTP health checker
func NewHTTPChecker(timeout time.Duration, path string) *HTTPChecker {
	return &HTTPChecker{
		Timeout: timeout,
		Path:    path,
	}
}

// Check performs an HTTP health check
func (c *HTTPChecker) Check(ctx context.Context, addr string) error {
	url := "http://" + addr + c.Path

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return err
	}

	client := &http.Client{
		Timeout: c.Timeout,
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return &HealthError{StatusCode: resp.StatusCode}
	}

	return nil
}

// TCPChecker checks if a TCP port is open
type TCPChecker struct {
	Timeout time.Duration
}

// NewTCPChecker creates a new TCP health checker
func NewTCPChecker(timeout time.Duration) *TCPChecker {
	return &TCPChecker{
		Timeout: timeout,
	}
}

// Check performs a TCP health check
func (c *TCPChecker) Check(ctx context.Context, addr string) error {
	dialer := &net.Dialer{
		Timeout: c.Timeout,
	}

	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	conn.Close()

	return nil
}

// HealthError represents a health check failure
type HealthError struct {
	StatusCode int
}

func (e *HealthError) Error() string {
	return "health check failed"
}

// Monitor monitors node health
type Monitor struct {
	checker   Checker
	interval  time.Duration
	results   map[string]bool
	resultsMu sync.RWMutex
}

// NewMonitor creates a new health monitor
func NewMonitor(checker Checker, interval time.Duration) *Monitor {
	return &Monitor{
		checker:  checker,
		interval: interval,
		results:  make(map[string]bool),
	}
}

// Check performs a health check
func (m *Monitor) Check(ctx context.Context, addr string) bool {
	err := m.checker.Check(ctx, addr)

	m.resultsMu.Lock()
	m.results[addr] = err == nil
	m.resultsMu.Unlock()

	return err == nil
}

// IsHealthy returns the health status of an address
func (m *Monitor) IsHealthy(addr string) bool {
	m.resultsMu.RLock()
	defer m.resultsMu.RUnlock()
	return m.results[addr]
}
