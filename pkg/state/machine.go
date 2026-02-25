// Package state provides a simple state machine for cluster nodes
package state

import (
	"errors"
	"sync"
)

// State represents the state of a node
type State string

const (
	StateOffline   State = "OFFLINE"
	StateFollower  State = "FOLLOWER"
	StateLeader    State = "LEADER"
)

// Transition represents a state transition
type Transition struct {
	From State
	To   State
}

// StateMachine manages state transitions
type StateMachine struct {
	transitions map[State][]State
	mu          sync.RWMutex
}

// NewStateMachine creates a new state machine
func NewStateMachine() *StateMachine {
	sm := &StateMachine{
		transitions: make(map[State][]State),
	}

	// Define valid transitions
	sm.transitions = map[State][]State{
		StateOffline:  {StateFollower},
		StateFollower: {StateLeader, StateOffline},
		StateLeader:   {StateFollower, StateOffline},
	}

	return sm
}

// CanTransition checks if a transition is valid
func (sm *StateMachine) CanTransition(from, to State) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	allowed, ok := sm.transitions[from]
	if !ok {
		return false
	}

	for _, s := range allowed {
		if s == to {
			return true
		}
	}

	return false
}

// Transition performs a state transition
func (sm *StateMachine) Transition(from, to State) error {
	if !sm.CanTransition(from, to) {
		return ErrInvalidTransition
	}
	return nil
}

// ErrInvalidTransition is returned for invalid state transitions
var ErrInvalidTransition = errors.New("invalid state transition")

// NodeStateManager manages state for cluster nodes
type NodeStateManager struct {
	sm      *StateMachine
	states  map[string]State
	mu      sync.RWMutex
}

// NewNodeStateManager creates a new node state manager
func NewNodeStateManager() *NodeStateManager {
	return &NodeStateManager{
		sm:     NewStateMachine(),
		states: make(map[string]State),
	}
}

// SetState sets the state of a node
func (nm *NodeStateManager) SetState(nodeID string, state State) error {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	current := nm.states[nodeID]

	if err := nm.sm.Transition(current, state); err != nil {
		return err
	}

	nm.states[nodeID] = state
	return nil
}

// GetState gets the state of a node
func (nm *NodeStateManager) GetState(nodeID string) State {
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	state, ok := nm.states[nodeID]
	if !ok {
		return StateOffline
	}
	return state
}

// GetStateCount returns the count of nodes in each state
func (nm *NodeStateManager) GetStateCount() map[State]int {
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	counts := make(map[State]int)
	for _, state := range nm.states {
		counts[state]++
	}
	return counts
}
