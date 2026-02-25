// Package sharding provides consistent hashing and partition management
package sharding

import (
	"fmt"
	"hash/crc32"
	"sort"
	"sync"
)

// NodeState represents the state of a node in the cluster
type NodeState string

const (
	StateOffline   NodeState = "OFFLINE"
	StateFollower  NodeState = "FOLLOWER"
	StateLeader    NodeState = "LEADER"
)

// Node represents a node in the cluster
type Node struct {
	ID             string
	Address        string
	State          NodeState
	Weight         int // For weighted consistent hashing
	PartitionIndex int // Used for partition assignment
}

func (n *Node) IsHealthy() bool {
	return n.State != StateOffline
}

// Partition represents a data partition
type Partition struct {
	ID         int
	Leader     *Node
	Followers  []*Node
	Version    int // For optimistic locking
}

// AssignmentResult contains the result of an assignment operation
type AssignmentResult struct {
	PartitionID int
	FromNode    *Node
	ToNode      *Node
	Success     bool
	Error       error
}

// PartitionMovement represents a planned partition movement
type PartitionMovement struct {
	PartitionID int
	FromNode   *Node
	ToNode     *Node
	State      MovementState
}

// MovementState represents the state of a partition movement
type MovementState string

const (
	MovementPending   MovementState = "PENDING"
	MovementStarted   MovementState = "STARTED"
	MovementCompleted MovementState = "COMPLETED"
	MovementFailed    MovementState = "FAILED"
)

// Cluster manages the sharding cluster
type Cluster struct {
	name       string
	numParts   int
	nodes      map[string]*Node
	partitions map[int]*Partition
	ring       []ringEntry // Sorted by hash for consistent hashing
	mu         sync.RWMutex
}

// ringEntry for consistent hashing
type ringEntry struct {
	hash uint32
	node *Node
}

const (
	defaultVNodes = 150 // Virtual nodes per real node
)

// NewCluster creates a new sharding cluster
func NewCluster(name string, numPartitions int) *Cluster {
	return &Cluster{
		name:       name,
		numParts:   numPartitions,
		nodes:      make(map[string]*Node),
		partitions: make(map[int]*Partition),
		ring:       make([]ringEntry, 0),
	}
}

// AddNode adds a node to the cluster
func (c *Cluster) AddNode(node *Node) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.nodes[node.ID]; exists {
		return ErrNodeExists
	}

	// Set default weight
	if node.Weight == 0 {
		node.Weight = 100
	}

	node.State = StateFollower
	c.nodes[node.ID] = node

	// Rebuild the ring
	c.rebuildRing()

	// Assign partitions
	c.assignPartitions()

	return nil
}

// AddNodeWithoutRebalance adds a node without triggering rebalancing
func (c *Cluster) AddNodeWithoutRebalance(node *Node) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.nodes[node.ID]; exists {
		return ErrNodeExists
	}

	if node.Weight == 0 {
		node.Weight = 100
	}

	node.State = StateFollower
	c.nodes[node.ID] = node
	c.rebuildRing()

	// NO assignment - partitions stay where they are

	return nil
}

// RemoveNode removes a node from the cluster
func (c *Cluster) RemoveNode(nodeID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.nodes[nodeID]; !exists {
		return ErrNodeNotFound
	}

	delete(c.nodes, nodeID)
	c.rebuildRing()
	c.assignPartitions()

	return nil
}

// GetPartition returns the partition ID for a given key
func (c *Cluster) GetPartition(key string) int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.ring) == 0 {
		return -1
	}

	hash := c.hashKey(key)

	// Binary search for the first ring entry with hash >= key hash
	idx := sort.Search(len(c.ring), func(i int) bool {
		return c.ring[i].hash >= hash
	})

	if idx >= len(c.ring) {
		idx = 0
	}

	// Use the node to determine partition
	node := c.ring[idx].node
	partID := node.PartitionIndex%c.numParts + c.numParts
	partID = partID % c.numParts
	
	return partID
}

// GetNodeForKey returns the primary node for a given key
func (c *Cluster) GetNodeForKey(key string) *Node {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.ring) == 0 {
		return nil
	}

	hash := c.hashKey(key)

	idx := sort.Search(len(c.ring), func(i int) bool {
		return c.ring[i].hash >= hash
	})

	if idx >= len(c.ring) {
		idx = 0
	}

	return c.ring[idx].node
}

// GetLeader returns the leader node for a partition
func (c *Cluster) GetLeader(partitionID int) *Node {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if part, ok := c.partitions[partitionID]; ok {
		return part.Leader
	}
	return nil
}

// GetNodes returns all nodes in the cluster
func (c *Cluster) GetNodes() []*Node {
	c.mu.RLock()
	defer c.mu.RUnlock()

	nodes := make([]*Node, 0, len(c.nodes))
	for _, n := range c.nodes {
		nodes = append(nodes, n)
	}
	return nodes
}

// GetPartitionCount returns the number of partitions
func (c *Cluster) GetPartitionCount() int {
	return c.numParts
}

// rebuildRing rebuilds the consistent hashing ring
func (c *Cluster) rebuildRing() {
	c.ring = make([]ringEntry, 0, len(c.nodes)*defaultVNodes)

	// Assign partition index to each node
	nodeList := make([]*Node, 0, len(c.nodes))
	for _, n := range c.nodes {
		nodeList = append(nodeList, n)
	}
	
	for i, node := range nodeList {
		node.PartitionIndex = i
		for j := 0; j < defaultVNodes; j++ {
			ringKey := fmt.Sprintf("%s-%d-%d", node.ID, node.Weight, j)
			entry := ringEntry{
				hash: c.hashKey(ringKey),
				node: node,
			}
			c.ring = append(c.ring, entry)
		}
	}

	// Sort by hash for binary search
	sort.Slice(c.ring, func(i, j int) bool {
		return c.ring[i].hash < c.ring[j].hash
	})
}

// assignPartitions assigns partitions to nodes
func (c *Cluster) assignPartitions() {
	// Reset all partitions
	for i := 0; i < c.numParts; i++ {
		c.partitions[i] = &Partition{ID: i}
	}

	if len(c.nodes) == 0 {
		return
	}

	// Get nodes directly from the map (already holding lock)
	nodesList := make([]*Node, 0, len(c.nodes))
	for _, n := range c.nodes {
		nodesList = append(nodesList, n)
	}

	// Simple round-robin assignment for now
	for i := 0; i < c.numParts; i++ {
		part := c.partitions[i]
		part.Leader = nodesList[i%len(nodesList)]
		part.Leader.State = StateLeader
	}
}

// hashKey generates a hash for a key
func (c *Cluster) hashKey(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

// ============================================================
// Assignment - Partition Assignment Management
// ============================================================

// AssignmentStrategy defines how partitions are assigned to nodes
type AssignmentStrategy string

const (
	StrategyRoundRobin    AssignmentStrategy = "ROUND_ROBIN"
	StrategyHashBased     AssignmentStrategy = "HASH_BASED"
	StrategyLoadBalanced  AssignmentStrategy = "LOAD_BALANCED"
)

// AssignPartitions assigns partitions to nodes based on the given strategy
func (c *Cluster) AssignPartitions(strategy AssignmentStrategy) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.nodes) == 0 {
		return ErrNoNodesAvailable
	}

	switch strategy {
	case StrategyRoundRobin:
		return c.assignRoundRobin()
	case StrategyHashBased:
		return c.assignHashBased()
	case StrategyLoadBalanced:
		return c.assignLoadBalanced()
	default:
		return c.assignRoundRobin()
	}
}

// assignRoundRobin assigns partitions in round-robin fashion
func (c *Cluster) assignRoundRobin() error {
	nodeList := c.getNodeList()
	if len(nodeList) == 0 {
		return ErrNoNodesAvailable
	}

	for i := 0; i < c.numParts; i++ {
		if c.partitions[i] == nil {
			c.partitions[i] = &Partition{ID: i}
		}
		c.partitions[i].Leader = nodeList[i%len(nodeList)]
		c.partitions[i].Leader.State = StateLeader
		c.partitions[i].Version++
	}

	return nil
}

// assignHashBased assigns partitions based on consistent hash
func (c *Cluster) assignHashBased() error {
	c.rebuildRing()

	for i := 0; i < c.numParts; i++ {
		if c.partitions[i] == nil {
			c.partitions[i] = &Partition{ID: i}
		}

		hash := uint32(i * 1000)
		idx := sort.Search(len(c.ring), func(j int) bool {
			return c.ring[j].hash >= hash
		})
		if idx >= len(c.ring) {
			idx = 0
		}
		if len(c.ring) > 0 {
			c.partitions[i].Leader = c.ring[idx].node
			c.partitions[i].Leader.State = StateLeader
			c.partitions[i].Version++
		}
	}

	return nil
}

// assignLoadBalanced assigns partitions to minimize load imbalance
func (c *Cluster) assignLoadBalanced() error {
	nodeList := c.getNodeList()
	if len(nodeList) == 0 {
		return ErrNoNodesAvailable
	}

	load := make(map[string]int)
	for _, n := range nodeList {
		load[n.ID] = 0
	}
	for _, p := range c.partitions {
		if p.Leader != nil {
			load[p.Leader.ID]++
		}
	}

	partitions := make([]int, 0, c.numParts)
	for i := 0; i < c.numParts; i++ {
		partitions = append(partitions, i)
	}

	for _, partID := range partitions {
		minLoadNode := nodeList[0]
		minLoad := load[minLoadNode.ID]
		for _, n := range nodeList {
			if load[n.ID] < minLoad {
				minLoad = load[n.ID]
				minLoadNode = n
			}
		}

		if c.partitions[partID] == nil {
			c.partitions[partID] = &Partition{ID: partID}
		}
		c.partitions[partID].Leader = minLoadNode
		c.partitions[partID].Leader.State = StateLeader
		c.partitions[partID].Version++
		load[minLoadNode.ID]++
	}

	return nil
}

// GetAssignment returns the current partition assignment
func (c *Cluster) GetAssignment() map[int]string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	assignment := make(map[int]string)
	for id, part := range c.partitions {
		if part.Leader != nil {
			assignment[id] = part.Leader.ID
		}
	}
	return assignment
}

// getNodeList returns nodes as a list (internal helper)
func (c *Cluster) getNodeList() []*Node {
	list := make([]*Node, 0, len(c.nodes))
	for _, n := range c.nodes {
		list = append(list, n)
	}
	return list
}

// ============================================================
// Partition Movement - Rebalancing
// ============================================================

// PlanMovement creates a movement plan to rebalance the cluster
func (c *Cluster) PlanMovement(targetStrategy AssignmentStrategy) ([]PartitionMovement, error) {
	c.mu.RLock()
	current := c.GetAssignment()
	c.mu.RUnlock()

	// Simulate target state
	temp := &Cluster{
		numParts:   c.numParts,
		nodes:      c.nodes,
		partitions: make(map[int]*Partition),
	}

	temp.assignPartitionsStrategy(targetStrategy)
	target := temp.GetAssignment()

	// Build movement plan
	plan := make([]PartitionMovement, 0)
	for partID, fromNodeID := range current {
		toNodeID := target[partID]
		if fromNodeID != toNodeID {
			fromNode := c.nodes[fromNodeID]
			toNode := c.nodes[toNodeID]
			plan = append(plan, PartitionMovement{
				PartitionID: partID,
				FromNode:    fromNode,
				ToNode:      toNode,
				State:       MovementPending,
			})
		}
	}

	return plan, nil
}

// assignPartitionsStrategy applies a strategy (internal helper)
func (c *Cluster) assignPartitionsStrategy(strategy AssignmentStrategy) {
	switch strategy {
	case StrategyRoundRobin:
		c.assignRoundRobin()
	case StrategyHashBased:
		c.assignHashBased()
	case StrategyLoadBalanced:
		c.assignLoadBalanced()
	}
}

// ExecuteMovement executes a partition movement
func (c *Cluster) ExecuteMovement(movement PartitionMovement) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	part, ok := c.partitions[movement.PartitionID]
	if !ok {
		return ErrPartitionNotFound
	}

	if movement.FromNode != nil && part.Leader != movement.FromNode {
		return ErrConcurrentModification
	}

	part.Leader = movement.ToNode
	part.Leader.State = StateLeader
	part.Version++

	return nil
}

// ExecuteMovementPlan executes a series of movements
func (c *Cluster) ExecuteMovementPlan(plan []PartitionMovement) []AssignmentResult {
	results := make([]AssignmentResult, 0, len(plan))

	for _, m := range plan {
		result := AssignmentResult{
			PartitionID: m.PartitionID,
			FromNode:    m.FromNode,
			ToNode:      m.ToNode,
		}

		err := c.ExecuteMovement(m)
		if err != nil {
			result.Success = false
			result.Error = err
		} else {
			result.Success = true
		}
		results = append(results, result)
	}

	return results
}

// Errors
var (
	ErrNodeExists            = &ClusterError{"node already exists"}
	ErrNodeNotFound          = &ClusterError{"node not found"}
	ErrNoNodesAvailable      = &ClusterError{"no nodes available"}
	ErrPartitionNotFound     = &ClusterError{"partition not found"}
	ErrConcurrentModification = &ClusterError{"concurrent modification detected"}
)

type ClusterError struct {
	msg string
}

func (e *ClusterError) Error() string {
	return e.msg
}

// GetName returns the cluster name
func (c *Cluster) GetName() string {
	return c.name
}

// GetNumPartitions returns the number of partitions
func (c *Cluster) GetNumPartitions() int {
	return c.numParts
}
