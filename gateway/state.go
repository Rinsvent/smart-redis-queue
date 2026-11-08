package gateway

import (
	"fmt"
	"sync"
)

// State — режим gateway (single-active v1).
type State int

const (
	StateBoot State = iota
	StateRunning
	StateRedisDown
	StateRecovering
	StateDraining
	StateFailed
)

func (s State) String() string {
	switch s {
	case StateBoot:
		return "boot"
	case StateRunning:
		return "running"
	case StateRedisDown:
		return "redis_down"
	case StateRecovering:
		return "recovering"
	case StateDraining:
		return "draining"
	case StateFailed:
		return "failed"
	default:
		return fmt.Sprintf("state(%d)", int(s))
	}
}

type stateMachine struct {
	mu sync.RWMutex
	s  State
}

func newStateMachine(initial State) *stateMachine {
	return &stateMachine{s: initial}
}

func (m *stateMachine) Get() State {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.s
}

func (m *stateMachine) set(next State) {
	m.mu.Lock()
	m.s = next
	m.mu.Unlock()
}

// allowPublish — новые publish только в Running.
func (m *stateMachine) allowPublish() error {
	switch m.Get() {
	case StateRunning:
		return nil
	case StateRedisDown:
		return ErrRedisDown
	case StateRecovering:
		return ErrRecovering
	case StateDraining:
		return ErrDraining
	default:
		return fmt.Errorf("gateway: publish not allowed in state %s", m.Get())
	}
}

// allowGet — выдача задач только в Running.
func (m *stateMachine) allowGet() error {
	return m.allowPublish()
}

// allowTerminal — Ack/Reject: Running или RedisDown (копим в WAL).
func (m *stateMachine) allowTerminal() error {
	switch m.Get() {
	case StateRunning, StateRedisDown, StateDraining:
		return nil
	case StateRecovering:
		return ErrRecovering
	default:
		return fmt.Errorf("gateway: ack/reject not allowed in state %s", m.Get())
	}
}
