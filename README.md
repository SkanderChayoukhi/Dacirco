# DaCirco: Transcoding-on-Demand Cloud Service

## Introduction
DaCirco is a cloud-based transcoding-on-demand service that leverages OpenStack and Kubernetes to manage video transcoding tasks. The system is designed to balance resource utilization and processing efficiency through advanced scheduling algorithms. It integrates RESTful backend requests, OpenStack virtual machines, and gRPC-based task dispatch for seamless and scalable operations.

## Project Goals
- Implement and test custom scheduling algorithms to optimize resource usage and processing times.
- Analyze and visualize performance metrics for algorithm comparison.
- Provide a robust platform for on-demand transcoding using cloud infrastructure.

## Architecture Overview
DaCirco's architecture consists of several key components:
1. **Controller**: Handles task scheduling and dispatch. It includes the main entry point (`main.py`), scheduling logic (`scheduler.py`), and worker management.
2. **Workers**: Virtual machines managed via OpenStack that perform transcoding tasks.
3. **Pod Manager**: Handles Kubernetes pods for dynamic scaling.
4. **Dataclasses**: Define entities like `TCRequest`, `TCWorker`, and `TCTask` for task tracking and worker management.
5. **Schedulers**: Implements custom algorithms like MinimizeWorkers, GracePeriod, and others to optimize task allocation.

## Scheduling Algorithms

1. #### Minimize Workers:

The Minimize Workers algorithm dynamically scales the number of workers to the minimum required to handle active tasks. It ensures that no idle workers remain in the system unless they are actively processing a task, thereby reducing resource consumption.

2. #### Grace Period:

The Grace Period algorithm introduces a delay before terminating idle workers, allowing for potential new requests to utilize these workers without incurring the cost of creating new ones. This approach balances resource efficiency with responsiveness to fluctuating workloads.

3. #### Scheduler with Queue:

The Scheduler with Queue algorithm adjusts the number of workers based on the size of the task queue. It:

- Adds new workers only when the number of waiting requests exceeds a defined threshold (queue_threshold).

- Removes idle workers when the queue is empty.

- Ensures efficient resource allocation while keeping minimal resources active to handle demand effectively.

**Key Features**:

- Dynamically scales workers based on queue size.

- Ensures a balance between resource usage and request processing.

4. #### Scheduler with Reserve:

The Scheduler with Reserve algorithm ensures there is always at least one reserve worker available to handle incoming requests. It:

- Creates new workers when demand exceeds capacity.

- Keeps one reserve idle worker available, provided the maximum worker limit is not reached.

- Removes excess idle workers if there are no waiting requests.

**Key Features**:

- Guarantees a reserve worker is available for immediate scaling.

- Balances responsiveness and resource efficiency.

5. #### Priority-Based Dynamic Scheduler:

This algorithm prioritizes tasks based on predefined criteria and dynamically adjusts worker allocation to handle higher-priority tasks more efficiently. It ensures critical tasks are processed promptly while maintaining overall system performance.

6. #### Adaptive Grace Period Algorithm:

An advanced version of the Grace Period algorithm, this approach adapts the delay period based on workload patterns and historical data. It fine-tunes resource allocation to optimize cost and responsiveness under varying workload conditions.

## Testing

Each algorithm is thoroughly tested with custom scripts to evaluate performance under different scenarios. Metrics such as resource utilization, task completion time, and system scalability are analyzed to ensure robust performance.



## Key Features
- **Cloud-Native**: Built using OpenStack for VM management and Kubernetes for container orchestration.
- **Scalable**: Dynamically allocates resources based on incoming workload.
- **Custom Scheduling**: Provides flexibility in task assignment strategies.
- **Performance Analysis**: Includes tools to measure and compare scheduling efficiency.

## Performance Metrics
Performance metrics, such as resource utilization and task completion times, are tracked and visualized using CSV files and graphs for detailed analysis.

