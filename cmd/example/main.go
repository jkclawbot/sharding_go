// Example demonstrates basic usage of tendril-go
package main

import (
	"fmt"
	"log"

	"github.com/jkclawbot/tendril-go/pkg/sharding"
)

func main() {
	fmt.Println("=== Tendril-Go Demo: Placement & Resources ===\n")

	// 1. Create a cluster with 10 partitions
	cluster := sharding.NewCluster("my-cluster", 10)
	fmt.Printf("Created cluster: %s with %d partitions\n\n", cluster.GetName(), cluster.GetNumPartitions())

	// 2. Add 5 nodes
	fmt.Println("Adding 5 nodes...")
	nodes := []struct {
		id      string
		address string
	}{
		{"node-1", "192.168.1.1:8080"},
		{"node-2", "192.168.1.2:8080"},
		{"node-3", "192.168.1.3:8080"},
		{"node-4", "192.168.1.4:8080"},
		{"node-5", "192.168.1.5:8080"},
	}

	for _, n := range nodes {
		cluster.AddNodeWithoutRebalance(&sharding.Node{
			ID:      n.id,
			Address: n.address,
			Weight: 100,
		})
	}
	fmt.Printf("Added %d nodes\n\n", len(nodes))

	// 3. Create a resource with default replication factor = 3
	fmt.Println("=== Resource Management ===")
	err := cluster.AddResource("user-data", 3)
	if err != nil {
		log.Printf("AddResource error: %v", err)
	}
	fmt.Println("Added resource 'user-data' with default replicas: 3")

	// Set custom replication for specific partitions
	cluster.SetPartitionReplicas("user-data", 0, 5)  // Partition 0 needs 5 replicas (critical)
	cluster.SetPartitionReplicas("user-data", 1, 2)  // Partition 1 only needs 2 replicas
	fmt.Println("Set custom replicas: partition 0 -> 5, partition 1 -> 2")

	// Verify
	fmt.Printf("Partition 0 replicas: %d\n", cluster.GetPartitionReplicas("user-data", 0))
	fmt.Printf("Partition 1 replicas: %d\n", cluster.GetPartitionReplicas("user-data", 1))
	fmt.Printf("Partition 2 replicas: %d (default)\n\n", cluster.GetPartitionReplicas("user-data", 2))

	// 4. Demonstrate Placement Strategies
	fmt.Println("=== Placement Strategies ===\n")

	// Even Distribution
	fmt.Println("1. EVEN_DISTRIBUTION (balanced across nodes):")
	results1 := cluster.PlacePartitions("user-data", sharding.PlacementEvenDistribution)
	printPlacementResults(results1, cluster)

	// Resource Aware - set some fake resource usage
	fmt.Println("\n2. RESOURCE_AWARE (considering node resources):")
	// Set resource usage on nodes
	nodeList := cluster.GetNodes()
	if len(nodeList) >= 2 {
		nodeList[0].SetResource("cpu", 80)  // High CPU
		nodeList[1].SetResource("cpu", 20)  // Low CPU
		fmt.Println("Set node-1 CPU: 80%, node-2 CPU: 20%")
	}
	results2 := cluster.PlacePartitions("user-data", sharding.PlacementResourceAware)
	printPlacementResults(results2, cluster)

	// Leader Aware
	fmt.Println("\n3. LEADER_AWARE (distribute leaders across nodes):")
	results3 := cluster.PlacePartitions("user-data", sharding.PlacementLeaderAware)
	printPlacementResults(results3, cluster)

	// Hash Based
	fmt.Println("\n4. HASH_BASED (consistent hashing):")
	results4 := cluster.PlacePartitions("user-data", sharding.PlacementHashBased)
	printPlacementResults(results4, cluster)

	// 5. Show load distribution for even distribution
	fmt.Println("\n=== Load Distribution (EVEN_DISTRIBUTION) ===")
	printLoadDistribution(results1, cluster)

	fmt.Println("\n=== Demo Complete ===")
}

func printPlacementResults(results []sharding.PlacementResult, cluster *sharding.Cluster) {
	leaderCount := make(map[string]int)
	replicaCount := make(map[string]int)

	for _, r := range results {
		if !r.Success {
			continue
		}
		if r.Leader != nil {
			leaderCount[r.Leader.ID]++
			replicaCount[r.Leader.ID]++
		}
		for _, f := range r.Followers {
			replicaCount[f.ID]++
		}
	}

	fmt.Println("Leaders per node:")
	for nodeID, count := range leaderCount {
		fmt.Printf("  %s: %d leaders\n", nodeID, count)
	}
	fmt.Println("Total replicas per node:")
	for nodeID, count := range replicaCount {
		fmt.Printf("  %s: %d replicas\n", nodeID, count)
	}
}

func printLoadDistribution(results []sharding.PlacementResult, cluster *sharding.Cluster) {
	nodeLoads := make(map[string]int)
	for _, r := range results {
		if !r.Success {
			continue
		}
		if r.Leader != nil {
			nodeLoads[r.Leader.ID]++
		}
	}

	fmt.Println("Partition load per node:")
	minLoad := 100
	maxLoad := 0
	total := 0
	for _, count := range nodeLoads {
		if count < minLoad {
			minLoad = count
		}
		if count > maxLoad {
			maxLoad = count
		}
		total += count
	}

	avg := float64(total) / float64(len(nodeLoads))
	for nodeID, count := range nodeLoads {
		balance := "✅"
		if count > int(avg)+1 || count < int(avg)-1 {
			balance = "⚠️"
		}
		fmt.Printf("  %s: %d partitions %s\n", nodeID, count, balance)
	}
	fmt.Printf("Balance: min=%d, max=%d, avg=%.1f\n", minLoad, maxLoad, avg)
}
