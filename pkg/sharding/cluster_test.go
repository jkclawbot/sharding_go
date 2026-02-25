package sharding

import (
	"testing"
)

func TestNewCluster(t *testing.T) {
	cluster := NewCluster("test-cluster", 100)
	if cluster.name != "test-cluster" {
		t.Errorf("expected name 'test-cluster', got '%s'", cluster.name)
	}
	if cluster.numParts != 100 {
		t.Errorf("expected 100 partitions, got %d", cluster.numParts)
	}
}

func TestAddNode(t *testing.T) {
	cluster := NewCluster("test", 10)

	node := &Node{
		ID:      "node-1",
		Address: "localhost:8080",
	}

	err := cluster.AddNode(node)
	if err != nil {
		t.Errorf("failed to add node: %v", err)
	}

	if len(cluster.nodes) != 1 {
		t.Errorf("expected 1 node, got %d", len(cluster.nodes))
	}
}

func TestGetPartition(t *testing.T) {
	cluster := NewCluster("test", 10)

	// Add nodes
	for i := 1; i <= 3; i++ {
		cluster.AddNode(&Node{
			ID:      "node-" + string(rune('0'+i)),
			Address: "localhost:808" + string(rune('0'+i)),
		})
	}

	// Same key should always return same partition
	p1 := cluster.GetPartition("user-123")
	p2 := cluster.GetPartition("user-123")
	if p1 != p2 {
		t.Errorf("same key returned different partitions: %d vs %d", p1, p2)
	}

	// Different keys may return different partitions (statistically)
	partitions := make(map[int]int)
	for i := 0; i < 100; i++ {
		p := cluster.GetPartition("key-" + string(rune('0'+i)))
		partitions[p]++
	}

	// Should have some distribution
	if len(partitions) == 0 {
		t.Error("no partitions assigned")
	}
}

func TestGetMaster(t *testing.T) {
	cluster := NewCluster("test", 10)

	// Should return nil when no nodes
	master := cluster.GetMaster(0)
	if master != nil {
		t.Error("expected nil master when no nodes")
	}

	// Add node
	cluster.AddNode(&Node{
		ID:      "node-1",
		Address: "localhost:8080",
	})

	master = cluster.GetMaster(0)
	if master == nil {
		t.Error("expected master after adding node")
	}
	if master.State != StateMaster {
		t.Errorf("expected state MASTER, got %s", master.State)
	}
}

func TestRemoveNode(t *testing.T) {
	cluster := NewCluster("test", 10)

	node := &Node{
		ID:      "node-1",
		Address: "localhost:8080",
	}
	cluster.AddNode(node)

	if len(cluster.nodes) != 1 {
		t.Errorf("expected 1 node, got %d", len(cluster.nodes))
	}

	cluster.RemoveNode("node-1")

	if len(cluster.nodes) != 0 {
		t.Errorf("expected 0 nodes after removal, got %d", len(cluster.nodes))
	}
}

func TestNodeState(t *testing.T) {
	node := &Node{
		ID:      "node-1",
		Address: "localhost:8080",
		State:   StateSlave,
	}

	if !node.IsHealthy() {
		t.Error("slave should be healthy")
	}

	node.State = StateOffline
	if node.IsHealthy() {
		t.Error("offline node should not be healthy")
	}
}
