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
	StateFollower NodeState = "FOLLOWER"
	StateLeader   NodeState = "LEADER"
)

// Node represents a node in the cluster
type Node struct {
	ID             string
	Address        string
	State          NodeState
	Weight         int // For weighted consistent hashing
	PartitionIndex int // Used for partition assignment
	Resources      map[string]int64 // Resource usage: CPU, Memory, etc.
}

func (n *Node) IsOnline() bool {
	return n.State != StateOffline
}

func (n *Node) GetResourceUsage(resource string) int64 {
	if n.Resources == nil {
		return 0
	}
	return n.Resources[resource]
}

func (n *Node) SetResource(resource string, value int64) {
	if n.Resources == nil {
		n.Resources = make(map[string]int64)
	}
	n.Resources[resource] = value
}

// Resource represents a resource with its configuration
type Resource struct {
	Name            string
	Partitions      map[int]*PartitionConfig // PartitionID -> Config
	DefaultReplicas int                      // Default replication factor
}

type PartitionConfig struct {
	Replicas      int            // Replication factor for this partition
	PreferNodes  []string       // Preferred nodes for this partition
	ExcludedNodes []string       // Excluded nodes for this partition
}

// NewResource creates a new resource with default replication factor
func NewResource(name string, defaultReplicas int) *Resource {
	return &Resource{
		Name:            name,
		Partitions:      make(map[int]*PartitionConfig),
		DefaultReplicas: defaultReplicas,
	}
}

// GetReplicas returns the replication factor for a partition
func (r *Resource) GetReplicas(partitionID int) int {
	if config, ok := r.Partitions[partitionID]; ok {
		return config.Replicas
	}
	return r.DefaultReplicas
}

// SetPartitionReplicas sets custom replication factor for a partition
func (r *Resource) SetPartitionReplicas(partitionID int, replicas int) {
	if r.Partitions[partitionID] == nil {
		r.Partitions[partitionID] = &PartitionConfig{}
	}
	r.Partitions[partitionID].Replicas = replicas
}

// Partition represents a data partition
type Partition struct {
	ID         int
	Leader     *Node
	Followers  []*Node
	Version    int // For optimistic locking
	Replicas   int // Replication factor for this partition
}

// Cluster manages the sharding cluster
type Cluster struct {
	name        string
	numParts    int
	nodes       map[string]*Node
	partitions  map[int]*Partition
	resources   map[string]*Resource // Resource name -> Resource
	ring        []ringEntry          // Sorted by hash for consistent hashing
	mu          sync.RWMutex
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
	if numPartitions <= 0 {
		numPartitions = 1 // Default to at least 1 partition
	}
	return &Cluster{
		name:       name,
		numParts:   numPartitions,
		nodes:      make(map[string]*Node),
		partitions: make(map[int]*Partition),
		resources:  make(map[string]*Resource),
		ring:       make([]ringEntry, 0),
	}
}

// AddNode adds a node to the cluster (with auto-assignment)
func (c *Cluster) AddNode(node *Node) error {
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
	idx := sort.Search(len(c.ring), func(i int) bool {
		return c.ring[i].hash >= hash
	})

	if idx >= len(c.ring) {
		idx = 0
	}

	node := c.ring[idx].node
	partID := node.PartitionIndex % c.numParts

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

// GetFollowers returns the follower nodes for a partition
func (c *Cluster) GetFollowers(partitionID int) []*Node {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if part, ok := c.partitions[partitionID]; ok {
		return part.Followers
	}
	return []*Node{}
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

// ============================================================
// Resource Management
// ============================================================

// AddResource adds a resource with default replication factor
func (c *Cluster) AddResource(name string, defaultReplicas int) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.resources[name]; exists {
		return ErrResourceExists
	}

	c.resources[name] = NewResource(name, defaultReplicas)
	return nil
}

// GetResource returns a resource by name
func (c *Cluster) GetResource(name string) *Resource {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.resources[name]
}

// SetResourceDefaultReplicas sets the default replication factor for a resource
func (c *Cluster) SetResourceDefaultReplicas(resourceName string, replicas int) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	res, ok := c.resources[resourceName]
	if !ok {
		return ErrResourceNotFound
	}

	res.DefaultReplicas = replicas
	return nil
}

// SetPartitionReplicas sets custom replication factor for a specific partition
func (c *Cluster) SetPartitionReplicas(resourceName string, partitionID int, replicas int) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	res, ok := c.resources[resourceName]
	if !ok {
		return ErrResourceNotFound
	}

	res.SetPartitionReplicas(partitionID, replicas)
	return nil
}

// GetPartitionReplicas returns the replication factor for a partition in a resource
func (c *Cluster) GetPartitionReplicas(resourceName string, partitionID int) int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	res, ok := c.resources[resourceName]
	if !ok {
		return 1 // Default
	}

	return res.GetReplicas(partitionID)
}

// ============================================================
// Placement Algorithms
// ============================================================

// PlacementStrategy defines different placement algorithms
type PlacementStrategy string

const (
	PlacementEvenDistribution PlacementStrategy = "EVEN_DISTRIBUTION"
	PlacementLeaderAware     PlacementStrategy = "LEADER_AWARE"
	PlacementResourceAware   PlacementStrategy = "RESOURCE_AWARE"
	PlacementHashBased       PlacementStrategy = "HASH_BASED"
)

// PlacementResult contains the result of a placement operation
type PlacementResult struct {
	PartitionID int
	Leader      *Node
	Followers   []*Node
	Replicas    int
	Success     bool
	Error       error
}

// PlacePartitions places partitions according to the strategy and resource config
func (c *Cluster) PlacePartitions(resourceName string, strategy PlacementStrategy) []PlacementResult {
	c.mu.Lock()
	defer c.mu.Unlock()

	res, ok := c.resources[resourceName]
	if !ok {
		return []PlacementResult{{Error: ErrResourceNotFound}}
	}

	results := make([]PlacementResult, 0, c.numParts)

	for i := 0; i < c.numParts; i++ {
		replicas := res.GetReplicas(i)
		result := PlacementResult{
			PartitionID: i,
			Replicas:    replicas,
		}

		var nodes []*Node
		switch strategy {
		case PlacementEvenDistribution:
			nodes = c.placeEvenDistribution(i, replicas)
		case PlacementLeaderAware:
			nodes = c.placeLeaderAware(i, replicas)
		case PlacementResourceAware:
			nodes = c.placeResourceAware(i, replicas, resourceName)
		case PlacementHashBased:
			nodes = c.placeHashBased(i, replicas)
		default:
			nodes = c.placeEvenDistribution(i, replicas)
		}

		if len(nodes) > 0 {
			result.Leader = nodes[0]
			if len(nodes) > 1 {
				result.Followers = nodes[1:]
			}
			result.Success = true
		}

		results = append(results, result)
	}

	return results
}

// placeEvenDistribution distributes replicas evenly across all nodes
func (c *Cluster) placeEvenDistribution(partitionID int, replicas int) []*Node {
	if len(c.nodes) == 0 {
		return nil
	}

	// Get nodes sorted by partition count (least loaded first)
	nodeList := c.getNodesByLoad()

	if len(nodeList) < replicas {
		replicas = len(nodeList)
	}

	selected := make([]*Node, 0, replicas)
	used := make(map[string]bool)
	
	// Distribute evenly: pick least loaded nodes first, avoid duplicates
	for i := 0; i < replicas; i++ {
		for _, node := range nodeList {
			if !used[node.ID] {
				selected = append(selected, node)
				used[node.ID] = true
				break
			}
		}
		// If all nodes used, cycle through (shouldn't happen with proper replica count)
		if len(selected) <= i {
			selected = append(selected, nodeList[i%len(nodeList)])
		}
	}

	return selected
}

// placeLeaderAware places leader on different nodes from existing leaders
func (c *Cluster) placeLeaderAware(partitionID int, replicas int) []*Node {
	if len(c.nodes) == 0 {
		return nil
	}

	// Find nodes that are not leaders for other partitions
	leaderNodes := make(map[string]bool)
	for _, p := range c.partitions {
		if p.Leader != nil {
			leaderNodes[p.Leader.ID] = true
		}
	}

	// Prefer nodes that are not leaders
	preferred := make([]*Node, 0)
	others := make([]*Node, 0)

	for _, n := range c.nodes {
		if !leaderNodes[n.ID] {
			preferred = append(preferred, n)
		} else {
			others = append(others, n)
		}
	}

	// Combine: preferred first, then others
	all := append(preferred, others...)

	if len(all) < replicas {
		replicas = len(all)
	}

	return all[:replicas]
}

// placeResourceAware considers node resource usage for placement
func (c *Cluster) placeResourceAware(partitionID int, replicas int, resourceName string) []*Node {
	if len(c.nodes) == 0 {
		return nil
	}

	// Sort by specified resource (lowest usage first)
	nodeList := make([]*Node, 0, len(c.nodes))
	for _, n := range c.nodes {
		nodeList = append(nodeList, n)
	}

	sort.Slice(nodeList, func(i, j int) bool {
		ri := nodeList[i].GetResourceUsage(resourceName)
		rj := nodeList[j].GetResourceUsage(resourceName)
		return ri < rj
	})

	if len(nodeList) < replicas {
		replicas = len(nodeList)
	}

	return nodeList[:replicas]
}

// placeHashBased uses consistent hashing for placement
func (c *Cluster) placeHashBased(partitionID int, replicas int) []*Node {
	if len(c.ring) == 0 || len(c.nodes) == 0 {
		return nil
	}

	selected := make(map[string]*Node)
	hash := uint32(partitionID * 1000)

	for len(selected) < replicas {
		idx := sort.Search(len(c.ring), func(i int) bool {
			return c.ring[i].hash >= hash
		})
		if idx >= len(c.ring) {
			idx = 0
		}

		node := c.ring[idx].node
		if _, exists := selected[node.ID]; !exists {
			selected[node.ID] = node
		}

		hash++
	}

	result := make([]*Node, 0, len(selected))
	for _, n := range selected {
		result = append(result, n)
	}

	return result
}

// getNodesByLoad returns nodes sorted by current partition load (least loaded first)
func (c *Cluster) getNodesByLoad() []*Node {
	load := make(map[string]int)
	for _, p := range c.partitions {
		if p.Leader != nil {
			load[p.Leader.ID]++
		}
		for _, f := range p.Followers {
			load[f.ID]++
		}
	}

	nodeList := make([]*Node, 0, len(c.nodes))
	for _, n := range c.nodes {
		nodeList = append(nodeList, n)
	}

	sort.Slice(nodeList, func(i, j int) bool {
		li := load[nodeList[i].ID]
		lj := load[nodeList[j].ID]
		return li < lj
	})

	return nodeList
}

// ============================================================
// Assignment - Partition Assignment Management
// ============================================================

// AssignmentStrategy defines how partitions are assigned to nodes
type AssignmentStrategy string

const (
	StrategyRoundRobin   AssignmentStrategy = "ROUND_ROBIN"
	StrategyHashBased   AssignmentStrategy = "HASH_BASED"
	StrategyLoadBalanced AssignmentStrategy = "LOAD_BALANCED"
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

// PartitionMovement represents a planned partition movement
type PartitionMovement struct {
	PartitionID int
	FromNode    *Node
	ToNode      *Node
	State       MovementState
}

// MovementState represents the state of a partition movement
type MovementState string

const (
	MovementPending   MovementState = "PENDING"
	MovementStarted   MovementState = "STARTED"
	MovementCompleted MovementState = "COMPLETED"
	MovementFailed    MovementState = "FAILED"
)

// AssignmentResult contains the result of an assignment operation
type AssignmentResult struct {
	PartitionID int
	FromNode    *Node
	ToNode      *Node
	Success     bool
	Error       error
}

// PlanMovement creates a movement plan to rebalance the cluster
func (c *Cluster) PlanMovement(targetStrategy AssignmentStrategy) ([]PartitionMovement, error) {
	c.mu.RLock()
	current := c.GetAssignment()
	
	// Deep copy nodes to avoid shared reference
	tempNodes := make(map[string]*Node)
	for k, v := range c.nodes {
		tempNodes[k] = v
	}
	
	c.mu.RUnlock()

	temp := &Cluster{
		numParts:   c.numParts,
		nodes:      tempNodes,
		partitions: make(map[int]*Partition),
	}

	temp.assignPartitionsStrategy(targetStrategy)
	target := temp.GetAssignment()

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

// ExecuteMovementPlan executes a series of movements atomically
func (c *Cluster) ExecuteMovementPlan(plan []PartitionMovement) []AssignmentResult {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	results := make([]AssignmentResult, 0, len(plan))

	for _, m := range plan {
		result := AssignmentResult{
			PartitionID: m.PartitionID,
			FromNode:    m.FromNode,
			ToNode:      m.ToNode,
		}

		part, ok := c.partitions[m.PartitionID]
		if !ok {
			result.Success = false
			result.Error = ErrPartitionNotFound
			results = append(results, result)
			continue
		}

		// Validate FromNode matches current leader
		if m.FromNode != nil && part.Leader != m.FromNode {
			result.Success = false
			result.Error = ErrConcurrentModification
			results = append(results, result)
			continue
		}

		// Execute movement
		part.Leader = m.ToNode
		part.Leader.State = StateLeader
		part.Version++
		result.Success = true
		results = append(results, result)
	}

	return results
}

// ============================================================
// Internal Helpers
// ============================================================

// rebuildRing rebuilds the consistent hashing ring
func (c *Cluster) rebuildRing() {
	c.ring = make([]ringEntry, 0, len(c.nodes)*defaultVNodes)

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

	sort.Slice(c.ring, func(i, j int) bool {
		return c.ring[i].hash < c.ring[j].hash
	})
}

// assignPartitions assigns partitions to nodes
func (c *Cluster) assignPartitions() {
	for i := 0; i < c.numParts; i++ {
		c.partitions[i] = &Partition{ID: i}
	}

	if len(c.nodes) == 0 {
		return
	}

	nodeList := make([]*Node, 0, len(c.nodes))
	for _, n := range c.nodes {
		nodeList = append(nodeList, n)
	}

	for i := 0; i < c.numParts; i++ {
		part := c.partitions[i]
		part.Leader = nodeList[i%len(nodeList)]
		part.Leader.State = StateLeader
	}
}

// hashKey generates a hash for a key
func (c *Cluster) hashKey(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

// ============================================================
// Errors
// ============================================================

var (
	ErrNodeExists             = &ClusterError{"node already exists"}
	ErrNodeNotFound           = &ClusterError{"node not found"}
	ErrNoNodesAvailable       = &ClusterError{"no nodes available"}
	ErrPartitionNotFound      = &ClusterError{"partition not found"}
	ErrConcurrentModification = &ClusterError{"concurrent modification detected"}
	ErrResourceExists        = &ClusterError{"resource already exists"}
	ErrResourceNotFound      = &ClusterError{"resource not found"}
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
