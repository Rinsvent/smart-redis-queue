package gateway

import "errors"

var (
	ErrRedisDown   = errors.New("gateway: redis unavailable")
	ErrRecovering  = errors.New("gateway: recovering")
	ErrDraining    = errors.New("gateway: draining")
	ErrPersist     = errors.New("gateway: redis persistence window too large or disabled")
	ErrNotRunning  = errors.New("gateway: not running")
	ErrEmptyQueue  = errors.New("gateway: queue name required")
	ErrEmptyTaskID = errors.New("gateway: task id required")
)
