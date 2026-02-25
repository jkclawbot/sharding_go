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
	ID     int
	Leader *Node
	Followers []*Node
}

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

// Errors
var (
	ErrNodeExists   = &ClusterError{"node already exists"}
	ErrNodeNotFound = &ClusterError{"node not found"}
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
