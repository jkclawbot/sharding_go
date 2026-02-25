// Example demonstrates basic usage of tendril-go
package main

import (
	"fmt"
	"log"

	"github.com/jkclawbot/tendril-go/pkg/sharding"
)

func main() {
	fmt.Println("=== Tendril-Go Demo ===\n")

	// 1. Create a cluster with 10 partitions (no auto-assignment)
	cluster := sharding.NewCluster("my-cluster", 10)
	fmt.Printf("Created cluster: %s with %d partitions\n\n", cluster.GetName(), cluster.GetNumPartitions())

	// 2. Add 2 nodes
	nodes := []*sharding.Node{
		{ID: "node-1", Address: "192.168.1.1:8080", Weight: 100},
		{ID: "node-2", Address: "192.168.1.2:8080", Weight: 100},
	}

	for _, node := range nodes {
		cluster.AddNodeWithoutRebalance(node)
		fmt.Printf("Added node: %s at %s (no rebalance)\n", node.ID, node.Address)
	}

	// 3. Assign partitions with 2 nodes
	fmt.Println("\n--- Assigning partitions (2 nodes) ---")
	cluster.AssignPartitions(sharding.StrategyRoundRobin)
	assignment := cluster.GetAssignment()
	printAssignment(assignment)

	// 4. Add 3rd node WITHOUT rebalancing (simulates imbalance)
	fmt.Println("\nAdding node-3 (no rebalance)...")
	cluster.AddNodeWithoutRebalance(&sharding.Node{ID: "node-3", Address: "192.168.1.3:8080", Weight: 100})

	// Show unbalanced assignment
	fmt.Println("\n--- Unbalanced Assignment (2 nodes assigned to 3) ---")
	assignment = cluster.GetAssignment()
	printAssignment(assignment)

	// Count per node
	nodeCount := make(map[string]int)
	for _, nodeID := range assignment {
		nodeCount[nodeID]++
	}
	fmt.Printf("Partitions per node: %v\n", nodeCount)

	// 5. Plan rebalancing to LoadBalanced
	fmt.Println("\n--- Planning Rebalancing ---")
	plan, err := cluster.PlanMovement(sharding.StrategyLoadBalanced)
	if err != nil {
		log.Printf("PlanMovement error: %v", err)
	} else {
		fmt.Printf("Planned movements: %d\n", len(plan))
		for _, m := range plan {
			fmt.Printf("  Partition %d: %s -> %s\n", m.PartitionID, m.FromNode.ID, m.ToNode.ID)
		}
	}

	// 6. Execute movements
	fmt.Println("\n--- Executing Movements ---")
	results := cluster.ExecuteMovementPlan(plan)
	successCount := 0
	for _, r := range results {
		if r.Success {
			successCount++
		} else {
			fmt.Printf("  FAILED: Partition %d: %v\n", r.PartitionID, r.Error)
		}
	}
	fmt.Printf("Executed: %d/%d successful\n", successCount, len(results))

	// 7. Show final balanced assignment
	fmt.Println("\n--- Final Assignment (balanced) ---")
	assignment = cluster.GetAssignment()
	printAssignment(assignment)

	nodeCount = make(map[string]int)
	for _, nodeID := range assignment {
		nodeCount[nodeID]++
	}
	fmt.Printf("Partitions per node: %v\n", nodeCount)

	fmt.Println("\n=== Demo Complete ===")
}

func printAssignment(assignment map[int]string) {
	for partID, nodeID := range assignment {
		fmt.Printf("  Partition %d -> %s\n", partID, nodeID)
	}
}
