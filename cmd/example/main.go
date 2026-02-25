// Example demonstrates basic usage of tendril-go
package main

import (
	"fmt"
	"log"

	"github.com/jkclawbot/tendril-go/pkg/sharding"
)

func main() {
	fmt.Println("=== Tendril-Go Demo ===\n")

	// 1. Create a cluster with 10 partitions
	cluster := sharding.NewCluster("my-cluster", 10)
	fmt.Printf("Created cluster: %s with %d partitions\n\n", cluster.GetName(), cluster.GetNumPartitions())

	// 2. Add nodes to the cluster
	nodes := []*sharding.Node{
		{ID: "node-1", Address: "192.168.1.1:8080", Weight: 100},
		{ID: "node-2", Address: "192.168.1.2:8080", Weight: 100},
		{ID: "node-3", Address: "192.168.1.3:8080", Weight: 100},
	}

	for _, node := range nodes {
		if err := cluster.AddNode(node); err != nil {
			log.Printf("Failed to add node %s: %v", node.ID, err)
		} else {
			fmt.Printf("Added node: %s at %s\n", node.ID, node.Address)
		}
	}

	fmt.Println()

	// 3. Demonstrate consistent hashing
	keys := []string{
		"user-100", "user-101", "user-102", "user-103", "user-104",
		"order-500", "order-501", "order-502", "order-503", "order-504",
		"product-1", "product-2", "product-3", "product-4", "product-5",
	}

	fmt.Println("Key to Partition Mapping (consistent hashing):")
	partitions := make(map[int]int)
	for _, key := range keys {
		part := cluster.GetPartition(key)
		leader := cluster.GetLeader(part)
		fmt.Printf("  Key '%s' -> Partition %d -> Leader: %s\n", key, part, leader.ID)
		partitions[part]++
	}

	fmt.Printf("\nPartition distribution: %v\n", partitions)

	fmt.Println("\n=== Demo Complete ===")
}
