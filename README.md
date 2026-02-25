# Tendril-Go

A lightweight distributed sharding library in Go, inspired by Apache Helix and Tendril.

## Core Features

- **Consistent Hashing** - Distribute keys across partitions
- **Partition Management** - Assign partitions to nodes
- **State Machine** - Simple MASTER/SLAVE/OFFLINE states
- **Health Checking** - Monitor node health
- **Rebalancing** - Auto-rebalance when nodes join/leave

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "log"
    
    "github.com/jkclawbot/tendril-go/pkg/sharding"
)

func main() {
    // Create a sharding cluster with 100 partitions
    cluster := sharding.NewCluster("my-cluster", 100)
    
    // Add nodes
    node1 := &sharding.Node{
        ID:      "node-1",
        Address: "192.168.1.1:8080",
    }
    node2 := &sharding.Node{
        ID:      "node-2", 
        Address: "192.168.1.2:8080",
    }
    
    cluster.AddNode(node1)
    cluster.AddNode(node2)
    
    // Get partition for a key
    partition := cluster.GetPartition("user-123")
    fmt.Printf("Key 'user-123' maps to partition %d\n", partition)
    
    // Get primary node for partition
    primary := cluster.GetMaster(partition)
    fmt.Printf("Partition %d master: %s\n", primary.ID, primary.Address)
}
```

## Architecture

```
┌─────────────────────────────────────────┐
│              Cluster                     │
│  ┌─────────────────────────────────┐   │
│  │     Partition Assignment         │   │
│  │  P0→Node1  P1→Node2  P2→Node1 │   │
│  └─────────────────────────────────┘   │
│  ┌─────────────────────────────────┐   │
│  │     State Machine               │   │
│  │  OFFLINE → SLAVE → MASTER      │   │
│  └─────────────────────────────────┘   │
└─────────────────────────────────────────┘
```

## Modules

- `pkg/sharding` - Core sharding logic
- `pkg/cluster` - Cluster management
- `pkg/state` - State machine
- `pkg/health` - Health checking
- `pkg/member` - Member discovery

## License

MIT
