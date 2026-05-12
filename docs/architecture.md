# PULSAR Architecture

This document provides a comprehensive overview of the PULSAR GPU Queue & Fairness Control Plane architecture, including the PULSAR control layer, the underlying KGWE execution engine, component interactions, data flows, and design decisions.

## Table of Contents

- [PULSAR Control Plane](#pulsar-control-plane)
- [System Overview](#system-overview)
- [Component Architecture](#component-architecture)
- [Data Flow](#data-flow)
- [GPU Topology Model](#gpu-topology-model)
- [Scheduling Algorithm](#scheduling-algorithm)
- [MIG Management](#mig-management)
- [Deployment Architecture](#deployment-architecture)
- [Security Architecture](#security-architecture)

---

## PULSAR Control Plane

PULSAR adds a queue and fairness control layer on top of the KGWE execution engine. All GPU workloads flow through PULSAR before reaching the underlying topology-aware scheduler.

### PULSAR Pipeline

```mermaid
flowchart LR
    subgraph "PULSAR Control Plane"
        QM["Queue Manager<br/>(per-user FIFO)"]
        FS["Fair Scheduler<br/>(DRF-based)"]
        AC["Admission Controller<br/>(quota + capacity)"]
        MC["Metrics Collector"]
    end

    subgraph "KGWE Execution Engine"
        SCHED["Topology-Aware<br/>Scheduler"]
        OPT["Workload<br/>Optimizer"]
    end

    Submit["Job Submission"] --> QM
    QM --> FS
    FS --> AC
    AC --> SCHED
    SCHED --> OPT
    OPT --> Execute["GPU Execution"]

    QM -.-> MC
    FS -.-> MC
    AC -.-> MC

    style QM fill:#7c3aed,color:#fff
    style FS fill:#7c3aed,color:#fff
    style AC fill:#7c3aed,color:#fff
    style MC fill:#7c3aed,color:#fff
    style SCHED fill:#76b900,color:#fff
    style OPT fill:#76b900,color:#fff
```

### PULSAR Components

| Component | Responsibility | Key Algorithm |
|-----------|---------------|---------------|
| **Queue Manager** | Per-user FIFO queues, fairness-weighted dequeuing | Round-robin with DRF scoring |
| **Fair Scheduler** | Tracks GPU usage, computes priority scores | `priority = 1/(1 + usage)` |
| **Admission Controller** | Enforces capacity limits and user quotas | Quota + cluster capacity check |
| **Metrics Collector** | Tracks job lifecycle, GPU-hours, wait times | Counter-based aggregation |
| **Control Plane** | Orchestrates the full pipeline | Event-driven orchestration |

---

## System Overview

The combined PULSAR + KGWE system is a Kubernetes-native platform that optimizes GPU resource utilization through queue-based fairness, topology-aware scheduling, ML-based workload prediction, and intelligent GPU partitioning.

```mermaid
graph TB
    subgraph "Kubernetes Cluster"
        subgraph "PULSAR Control Plane"
            QM[Queue Manager]
            FS[Fair Scheduler]
            AC[Admission Controller]
        end

        subgraph "KGWE Execution Engine"
            API[Kubernetes API Server]
            KS[kube-scheduler]
            KGWE[KGWE Scheduler Extender]
            CTRL[Controller]
            OPT["Optimizer (ML)"]
            MIG[MIG Controller]
            COST[Cost Engine]
            DISC[Discovery Service]
        end

        subgraph "GPU Nodes"
            N1["Node 1 (8x H100)"]
            N2["Node 2 (8x H100)"]
            N3["Node N (8x H100)"]
            A1[KGWE Agent]
            A2[KGWE Agent]
            A3[KGWE Agent]
        end
    end

    QM --> FS --> AC --> KGWE
    API --> KS
    KS --> KGWE
    KGWE --> CTRL
    CTRL --> OPT
    CTRL --> MIG
    CTRL --> COST
    CTRL --> DISC

    DISC --> A1
    DISC --> A2
    DISC --> A3

    A1 --> N1
    A2 --> N2
    A3 --> N3

    style QM fill:#7c3aed,color:#fff
    style FS fill:#7c3aed,color:#fff
    style AC fill:#7c3aed,color:#fff
    style KGWE fill:#76b900
    style CTRL fill:#76b900
    style OPT fill:#76b900
    style MIG fill:#76b900
```

---

## Component Architecture

### Core Components

```mermaid
graph LR
    subgraph "KGWE Components"
        subgraph "Scheduling Layer"
            SE[Scheduler Extender]
            TA[Topology Analyzer]
            GS[Gang Scheduler]
        end

        subgraph "Intelligence Layer"
            WO[Workload Optimizer]
            WC[Workload Classifier]
            RP[Resource Predictor]
        end

        subgraph "Resource Layer"
            MC[MIG Controller]
            MPS[MPS Controller]
            DS[Discovery Service]
        end

        subgraph "Operations Layer"
            CE[Cost Engine]
            ME[Metrics Exporter]
            AL[Alert Manager]
        end
    end

    SE --> TA
    SE --> GS
    SE --> WO

    WO --> WC
    WO --> RP

    SE --> MC
    SE --> MPS
    MC --> DS

    CE --> ME
    ME --> AL
```

### Component Responsibilities

| Component | Responsibility | Technology |
|-----------|---------------|------------|
| **Scheduler Extender** | Intercepts scheduling decisions, applies GPU-aware scoring | Go |
| **Topology Analyzer** | Maps GPU interconnects (NVLink, PCIe, NUMA) | Go + NVML |
| **Workload Optimizer** | ML-based resource prediction | Python |
| **MIG Controller** | Manages MIG partition lifecycle | Go |
| **Cost Engine** | Tracks usage, manages budgets | Go |
| **Discovery Service** | Real-time GPU topology discovery | Go + NVML |
| **Metrics Exporter** | Prometheus metrics | Go |

---

## Data Flow

### Scheduling Flow

```mermaid
sequenceDiagram
    autonumber
    participant User
    participant API as K8s API
    participant Sched as kube-scheduler
    participant Ext as KGWE Extender
    participant Opt as Optimizer
    participant Disc as Discovery
    participant Agent as Node Agent

    User->>API: Create GPUWorkload CR
    API->>Sched: Pod pending
    Sched->>Ext: Filter & Score nodes

    Ext->>Disc: Get cluster topology
    Disc->>Agent: Query GPU status
    Agent-->>Disc: GPU topology + health
    Disc-->>Ext: Topology data

    Ext->>Opt: Get placement hint
    Opt-->>Ext: ML recommendation

    Ext-->>Sched: Scored nodes
    Sched->>API: Bind pod to node

    API->>Agent: Pod scheduled
    Agent->>Agent: Configure MIG/MPS
    Agent-->>API: Pod running
```

### Telemetry Flow

```mermaid
flowchart LR
    subgraph "GPU Nodes"
        DCGM[DCGM Exporter]
        Agent[KGWE Agent]
    end

    subgraph "KGWE Control Plane"
        Disc[Discovery]
        Opt[Optimizer]
        Cost[Cost Engine]
        Exp[Metrics Exporter]
    end

    subgraph "Observability"
        Prom[Prometheus]
        Graf[Grafana]
        Alert[Alertmanager]
    end

    DCGM --> Agent
    Agent --> Disc
    Disc --> Opt
    Disc --> Cost

    Cost --> Exp
    Exp --> Prom
    Prom --> Graf
    Prom --> Alert

    style Prom fill:#e6522c
    style Graf fill:#f46800
```

---

## GPU Topology Model

### Topology Hierarchy

```mermaid
graph TB
    subgraph "Cluster Topology"
        Cluster[Cluster]

        subgraph "Node Level"
            Node1[Node 1]
            Node2[Node 2]
        end

        subgraph "GPU Level - Node 1"
            GPU0[GPU 0]
            GPU1[GPU 1]
            GPU2[GPU 2]
            GPU3[GPU 3]
        end

        subgraph "Interconnect"
            NVS[NVSwitch]
        end

        subgraph "MIG Level"
            MIG0[1g.10gb]
            MIG1[2g.20gb]
            MIG2[3g.40gb]
        end
    end

    Cluster --> Node1
    Cluster --> Node2

    Node1 --> GPU0
    Node1 --> GPU1
    Node1 --> GPU2
    Node1 --> GPU3

    GPU0 <--> NVS
    GPU1 <--> NVS
    GPU2 <--> NVS
    GPU3 <--> NVS

    GPU0 --> MIG0
    GPU0 --> MIG1
    GPU1 --> MIG2

    style NVS fill:#76b900
```

### NVLink Topology Matrix

```mermaid
graph LR
    subgraph "8-GPU DGX Node (NVSwitch)"
        G0((GPU 0))
        G1((GPU 1))
        G2((GPU 2))
        G3((GPU 3))
        G4((GPU 4))
        G5((GPU 5))
        G6((GPU 6))
        G7((GPU 7))
    end

    G0 <-->|900 GB/s| G1
    G0 <-->|900 GB/s| G2
    G0 <-->|900 GB/s| G4
    G1 <-->|900 GB/s| G3
    G1 <-->|900 GB/s| G5
    G2 <-->|900 GB/s| G3
    G2 <-->|900 GB/s| G6
    G3 <-->|900 GB/s| G7
    G4 <-->|900 GB/s| G5
    G4 <-->|900 GB/s| G6
    G5 <-->|900 GB/s| G7
    G6 <-->|900 GB/s| G7

    style G0 fill:#76b900
    style G1 fill:#76b900
    style G2 fill:#76b900
    style G3 fill:#76b900
    style G4 fill:#76b900
    style G5 fill:#76b900
    style G6 fill:#76b900
    style G7 fill:#76b900
```

---

## Scheduling Algorithm

### Scoring Pipeline

```mermaid
flowchart TB
    subgraph "Input"
        Pod[GPUWorkload]
        Topo[Cluster Topology]
    end

    subgraph "Filtering Phase"
        F1[GPU Count Filter]
        F2[Memory Filter]
        F3[Architecture Filter]
        F4[MIG Availability Filter]
    end

    subgraph "Scoring Phase"
        S1[Topology Score<br/>Weight: 40%]
        S2[Resource Score<br/>Weight: 35%]
        S3[Balance Score<br/>Weight: 25%]
    end

    subgraph "Optimization"
        ML[ML Placement Hint]
        Gang[Gang Scheduling]
    end

    subgraph "Output"
        Decision[Scheduling Decision]
    end

    Pod --> F1
    Topo --> F1
    F1 --> F2 --> F3 --> F4

    F4 --> S1
    F4 --> S2
    F4 --> S3

    S1 --> ML
    S2 --> ML
    S3 --> ML

    ML --> Gang
    Gang --> Decision
```

### Topology Scoring Algorithm

```mermaid
flowchart LR
    subgraph "Topology Preference"
        None[None<br/>Base: 50]
        NVLink[NVLink Optimal<br/>Base: 50-100]
        NUMA[Same NUMA<br/>Base: 50-90]
        PCIe[Same PCIe Switch<br/>Base: 50-80]
    end

    subgraph "Scoring Factors"
        BW[Bandwidth<br/>+0-50 points]
        Lat[Latency<br/>+0-20 points]
        Aff[Affinity<br/>+0-10 points]
    end

    subgraph "Final Score"
        FS[0-100]
    end

    NVLink --> BW
    NUMA --> Lat
    PCIe --> Aff

    BW --> FS
    Lat --> FS
    Aff --> FS
```

---

## MIG Management

### MIG Lifecycle

```mermaid
stateDiagram-v2
    [*] --> GPUDiscovered

    GPUDiscovered --> MIGEnabled: Enable MIG Mode
    MIGEnabled --> PartitionCreated: Create Partition

    PartitionCreated --> Available: Ready
    Available --> Allocated: Workload Scheduled
    Allocated --> InUse: Pod Running

    InUse --> Available: Pod Completed
    Available --> PartitionDestroyed: Rebalance

    PartitionDestroyed --> PartitionCreated: Create New Partition
    PartitionDestroyed --> MIGEnabled: Cleanup

    MIGEnabled --> GPUDiscovered: Disable MIG Mode
```

### MIG Profile Distribution

```mermaid
pie title "Inference-Optimized MIG Strategy"
    "1g.10gb (Small Inference)" : 70
    "2g.20gb (Medium Models)" : 20
    "3g.40gb (Large Models)" : 10
```

---

## Deployment Architecture

### Production Deployment

```mermaid
graph TB
    subgraph "Kubernetes Cluster"
        subgraph "kgwe-system Namespace"
            subgraph "Deployments"
                Ctrl[Controller<br/>1 replica]
                Sched[Scheduler<br/>2 replicas]
                Opt[Optimizer<br/>1 replica]
                MIG[MIG Controller<br/>1 replica]
                Cost[Cost Engine<br/>1 replica]
            end

            subgraph "DaemonSets"
                Disc[Discovery<br/>per GPU node]
                Agent[Agent<br/>per GPU node]
                Exp[Exporter<br/>per node]
            end

            subgraph "ConfigMaps & Secrets"
                CM[Scheduler Config]
                Sec[API Keys]
            end
        end

        subgraph "monitoring Namespace"
            Prom[Prometheus]
            Graf[Grafana]
        end
    end

    Ctrl --> Sched
    Sched --> Opt
    Sched --> MIG
    Ctrl --> Cost

    Disc --> Agent
    Agent --> Exp
    Exp --> Prom
    Prom --> Graf
```

### High Availability

```mermaid
graph LR
    subgraph "HA Configuration"
        subgraph "Leader Election"
            Ctrl1[Controller<br/>Leader]
            Ctrl2[Controller<br/>Standby]
        end

        subgraph "Load Balanced"
            Sched1[Scheduler 1]
            Sched2[Scheduler 2]
            LB[Service]
        end

        subgraph "Shared State"
            Etcd[(etcd)]
            CRD[CRDs]
        end
    end

    Ctrl1 <-.-> Ctrl2
    LB --> Sched1
    LB --> Sched2

    Ctrl1 --> Etcd
    Ctrl2 --> Etcd
    Sched1 --> CRD
    Sched2 --> CRD
```

---

## Security Architecture

### RBAC Model

```mermaid
graph TB
    subgraph "Service Accounts"
        SA1[kgwe-controller]
        SA2[kgwe-scheduler]
        SA3[kgwe-agent]
    end

    subgraph "Cluster Roles"
        CR1[Controller Role]
        CR2[Scheduler Role]
        CR3[Agent Role]
    end

    subgraph "Resources"
        Nodes[Nodes]
        Pods[Pods]
        CRDs[GPUWorkloads]
        Config[ConfigMaps]
    end

    SA1 --> CR1
    SA2 --> CR2
    SA3 --> CR3

    CR1 --> Nodes
    CR1 --> Pods
    CR1 --> CRDs
    CR2 --> Pods
    CR2 --> CRDs
    CR3 --> Nodes
    CR3 --> Config
```

### Network Security

```mermaid
graph LR
    subgraph "External"
        User[User]
        Prom[Prometheus]
    end

    subgraph "KGWE Namespace"
        API[API Server<br/>:8443]
        Metrics[Exporter<br/>:9400]
        GRPC[Optimizer<br/>:50051]
    end

    subgraph "GPU Nodes"
        Agent[Agent<br/>:50052]
    end

    User -->|HTTPS| API
    Prom -->|HTTP| Metrics
    API -->|gRPC/TLS| GRPC
    API -->|gRPC/TLS| Agent

    style API fill:#76b900
```

---

## Design Decisions

### Key Architectural Decisions

| Decision | Rationale | Trade-offs |
|----------|-----------|------------|
| **Scheduler Extender** | Extends default scheduler without replacement | Limited to scoring phase |
| **Python for ML** | Rich ML ecosystem (NumPy, scikit-learn) | Additional runtime dependency |
| **Go for core** | Performance, K8s native ecosystem | Steeper learning curve |
| **gRPC for IPC** | Performance, strong typing | More complex than REST |
| **CRDs for config** | Native K8s UX, validation | Learning curve for users |

### Performance Considerations

- **Caching**: Topology data cached with 30s TTL
- **Async operations**: MIG provisioning is non-blocking
- **Batch processing**: Telemetry aggregated before ML inference
- **Connection pooling**: gRPC channels reused

---

## Further Reading

- [PRD](PRD.md) - Product Requirements Document
- [API Reference](api-reference.md) - Custom Resource specifications
- [Operations Guide](operations.md) - Deployment and maintenance
