# Apollo: A Raft-Backed Distributed MapReduce Control Plane

Apollo is a lightweight distributed MapReduce control-plane implementation written in C++17. It uses real gRPC services over localhost to model communication between clients, replicated primary nodes, and workers. For primary failover, Apollo uses a simplified Raft-style replicated metadata log designed for the lab test environment.

The project focuses on the core distributed-systems behavior described in the proposal:

- gRPC-based communication between clients, primaries, and workers
- Replicated primary metadata with leader failover
- Worker registration and heartbeat-based liveness tracking
- Deterministic task lifecycle transitions
- Task reassignment after worker failure
- Speculative execution for slow workers
- Dynamic worker joins while jobs are already running
- Concurrent multi-job scheduling with isolated metadata
- Correct MapReduce execution for the built-in word-count workload
- Automated build and test execution through `make all`

---

## Team Information
- **Name:** R Yeeshu Dhurandhar, Abhiram Gottumukkala
- **AndrewID:** rdhurand, agottumu
- **Course:** 14-736 Distributed Systems: Techniques, Infrastructure, and Services
- **Mentor:** Prof. Patrick Tague 
---

## Project Overview

Apollo models a small distributed MapReduce cluster. A client submits a job to the current primary leader. The primary partitions the input into map tasks, schedules those tasks on available workers, waits for all map tasks to complete, creates reduce tasks, and finally merges the reduce outputs into a deterministic final result.

The cluster consists of three main components:

### 1. Client / CLI

The user-facing entry point for submitting a word-count job. The CLI starts a local Apollo cluster, loads an input file, submits the job, waits for completion, and prints the output.

### 2. Replicated Primary Control Plane

The primary service manages job metadata, task state, worker membership, task assignment, worker failure handling, speculative execution, and final result collection. Multiple primary replicas are started in the lab harness. One primary acts as the leader, while the others receive replicated metadata commands and can take over after leader failure.

### 3. Workers

Workers register with the current primary leader, send periodic heartbeats, and execute map or reduce tasks through gRPC. Workers can crash, restart, or join dynamically while jobs are already running.

---

## Design Overview

Apollo is structured as a small distributed MapReduce control plane. The test harness runs all nodes in one process, but each primary and worker exposes a real localhost gRPC service. This allows the implementation to exercise realistic RPC-based coordination while remaining easy to run in the lab environment.

The system has three main components:

1. **Client / CLI**

   The client submits a word-count job to the current primary leader. The demo CLI reads an input file, starts a local Apollo cluster, adds workers, submits the job, waits for completion, and prints the final output.

2. **Replicated Primary Control Plane**

   The primary leader manages job metadata, worker membership, task scheduling, task completion, failure handling, speculative execution, and result collection. Primary metadata is replicated through a simplified Raft-style command log. If the leader fails, a surviving primary can become the new leader and continue from replicated scheduling state.

3. **Workers**

   Workers register with the current leader, send periodic heartbeats, and execute map or reduce tasks through gRPC. Workers can join dynamically, crash, restart, and receive reassigned or speculative tasks.

The main control flow is:

```text
Client submits job
        |
        v
Primary leader records job in replicated metadata
        |
        v
Input is partitioned into map tasks
        |
        v
Scheduler assigns map tasks to workers
        |
        v
Workers execute map tasks and return intermediate counts
        |
        v
Primary creates reduce tasks after all maps complete
        |
        v
Scheduler assigns reduce tasks to workers
        |
        v
Workers merge reduce buckets
        |
        v
Primary commits final output and marks job complete
```

The main fault-tolerance flow is:
```text
Worker or leader failure occurs
        |
        v
Primary detects failure or new leader is elected
        |
        v
Unfinished in-flight attempts are failed or requeued
        |
        v
Scheduler assigns remaining work to healthy workers
        |
        v
Job completes without corrupting final output
```
---

## Repository Structure

```text
.
├── include/
│   └── apollo/
│       └── cluster.h          # Public ApolloCluster API
├── proto/
│   └── apollo.proto           # gRPC and Protocol Buffer definitions
├── src/
│   ├── cluster.cpp            # Cluster runtime, scheduler, workers, primaries
│   └── main.cpp               # Demo CLI
├── tests/
│   └── apollo_tests.cpp       # GoogleTest integration tests
├── CMakeLists.txt             # CMake build configuration
├── Makefile                   # Dependency setup, build, and test target
└── README.md
```

---

## Requirements

Apollo is built with:

- C++17
- CMake
- Protocol Buffers
- gRPC
- GoogleTest
- POSIX threads through the C++ standard threading library

The provided Makefile attempts to install required dependencies automatically on supported systems.

**Supported package-manager paths:**
- macOS with Homebrew
- Ubuntu/Debian with apt-get

If the testing environment does not allow package installation, the dependencies must already be available in the environment.

---

## Build and Test

To install dependencies when possible, configure the project, build all targets, and run the test suite:

```bash
make all
```

This runs the following high-level steps:

```
make deps
make configure
make build
make test
```

Equivalent manual commands are:

```bash
cmake -S . -B build
cmake --build build
ctest --test-dir build --output-on-failure
```

The build generates C++ sources from `proto/apollo.proto`, builds the Apollo library, builds the demo CLI, builds the GoogleTest test binary, and runs the tests through CTest.

> If a local GoogleTest installation is not discoverable, the CMake build falls back to fetching GoogleTest automatically.

### Cleaning the Build

To remove the generated build directory:

```bash
make clean
```

---

## Demo CLI

Run a local word-count job against a text file:

```bash
./build/apollo_cli demo-wordcount path/to/input.txt
```

**Example:**

```bash
echo "cat dog cat bird dog cat" > input.txt
./build/apollo_cli demo-wordcount input.txt
```

**Expected output format:**

```
job_id=job-1
bird 1
cat 3
dog 2
```

> The exact job ID may vary depending on the cluster state, but the word-count output is deterministic.

---

## Public API

The main public API is exposed through `include/apollo/cluster.h`.

**Minimal example:**

```cpp
#include "apollo/cluster.h"

#include <chrono>
#include <iostream>

int main() {
    apollo::ApolloCluster cluster;
    cluster.start();

    cluster.addWorker({"worker-1", 1.0});
    cluster.addWorker({"worker-2", 1.0});
    cluster.addWorker({"worker-3", 1.0});

    const auto job_id = cluster.submitWordCountJob({
        "example-wordcount",
        "cat dog cat bird dog cat",
        3,
        2,
    });

    const auto result = cluster.waitForJobResult(job_id, std::chrono::seconds(5));
    std::cout << apollo::formatWordCount(result);

    return 0;
}
```

---

## Architecture

### Client and Job Submission

A client submits a `SubmitJobRequest` to the current primary leader. The leader assigns a unique job ID, records the job in the replicated metadata log, partitions the input, and creates map task metadata.

If a client reaches a non-leader primary, the primary returns a leader hint so the client can retry against the current leader.

### Primary Replicas

Apollo starts multiple primary replicas in the lab harness. The first primary is initially selected as leader for deterministic startup. The leader is responsible for:

- accepting job submissions
- accepting worker registrations
- receiving worker heartbeats
- scheduling map and reduce tasks
- detecting failed workers
- launching speculative attempts
- committing task completions
- serving job status/result queries

Followers receive replicated metadata commands through the simplified consensus service. If the leader fails, a surviving follower can be elected and continue scheduling from replicated metadata.

### Simplified Raft-Style Metadata Replication

Apollo uses a simplified Raft-style replicated command log for primary metadata. The replicated commands include:

- worker registration
- job submission
- task assignment
- task completion
- worker failure

Each primary applies committed commands to its local control-plane state machine. This ensures that a new leader can recover job metadata, task states, worker membership, and committed completions after leader failure.

> This implementation is intentionally scoped for the lab failure model. It is not intended to be a production-grade Raft implementation. In particular, it uses a simplified full-log `AppendEntries` path instead of implementing every Raft optimization such as per-follower `nextIndex`, incremental conflict repair, durable persistence, or snapshotting.

### Workers

Each worker runs a real localhost gRPC server. A worker:

1. Starts its server
2. Registers with the current leader
3. Sends periodic heartbeat RPCs
4. Receives task execution RPCs
5. Executes map or reduce work
6. Returns results to the primary

Workers can be added dynamically while a job is running. Newly added workers register with the current leader and become eligible for future assignments.

### Scheduler

The scheduler runs in a background thread on the leader. On each iteration, it:

1. Detects workers whose heartbeats have timed out
2. Replicates worker-failure commands
3. Finds slow running tasks eligible for speculative execution
4. Assigns queued or requeued tasks to healthy available workers
5. Replicates task-assignment commands
6. Dispatches committed task attempts to workers over gRPC

Map tasks are scheduled first. Reduce tasks are created and scheduled only after all map tasks in the job have completed.

### Task Lifecycle

Each task follows a deterministic lifecycle:

```
Queued → Running → Completed
Queued → Running → Failed/Requeued → Running → Completed
```

A task may also have multiple attempts because of worker failure, leader failover, or speculative execution. Only one successful attempt is committed as authoritative. Late duplicate completions are ignored or marked failed.

---

## Fault Tolerance

Apollo handles two main failure scenarios:

### Worker Failure

If a worker crashes or misses heartbeats, the primary marks the worker unavailable. If the worker had an active task attempt, that attempt is failed and the logical task is requeued. A later scheduler iteration assigns the task to another healthy worker.

### Primary Leader Failure

If the current primary leader crashes, a surviving primary can become the new leader. The new leader resumes scheduling from the replicated metadata log. Running tasks are conservatively requeued after failover because their completion RPCs may have been sent to the old leader.

Completed tasks remain completed, so this recovery action is safe and idempotent.

---

## Speculative Execution

Apollo supports speculative execution for straggler mitigation. If a running task exceeds the configured speculative threshold and another healthy worker is available, the primary launches a duplicate attempt of the same logical task.

The first successful completion is accepted. Later duplicate completions for the same logical task are ignored so the final output remains correct.

---

## Multi-Job Isolation

Each submitted job owns separate metadata:

- Input partitions
- Map task records
- Reduce task records
- Task attempts
- Intermediate outputs
- Final output

This prevents cross-job contamination when multiple clients submit jobs concurrently.

---

## Word-Count Workload

Apollo currently implements a built-in word-count workload.

**Tokenization rules:**
- Alphanumeric characters form words
- Words are lowercased
- Non-alphanumeric characters separate words

Map tasks count words in their assigned input partition. Reduce tasks merge intermediate counts for a hash bucket. The final output is a sorted map from word to count.

---

## Configuration

The cluster can be configured through `ClusterConfig`:

```cpp
apollo::ApolloCluster cluster({
    std::chrono::milliseconds(50),   // heartbeat_interval
    std::chrono::milliseconds(180),  // worker_timeout
    std::chrono::milliseconds(20),   // scheduler_interval
    std::chrono::milliseconds(220),  // speculative_threshold
    3,                               // primary_replica_count
});
```

| Field | Meaning |
|---|---|
| `heartbeat_interval` | How often workers send heartbeat RPCs |
| `worker_timeout` | How long before a missing heartbeat marks a worker failed |
| `scheduler_interval` | How often the scheduler checks for work |
| `speculative_threshold` | How long a task can run before speculative execution is considered |
| `primary_replica_count` | Number of primary replicas in the control-plane cluster |

Worker speed can also be configured:

```cpp
cluster.addWorker({"worker-slow", 5.0});
cluster.addWorker({"worker-fast", 1.0});
```

> A larger `speed_multiplier` makes a worker slower. This is used by tests to create straggler scenarios.

---

## Built Code Documentation

The implementation includes code-level documentation across the main built components of Apollo.

| File | Documentation Coverage |
|---|---|
| `include/apollo/cluster.h` | Documents the public API, cluster configuration, worker configuration, job request/summary types, task states, job states, and helper functions. |
| `src/cluster.cpp` | Documents the internal runtime, primary replicas, worker runtimes, scheduler loop, consensus loop, replicated metadata commands, task lifecycle transitions, worker failure handling, leader failover handling, and speculative execution path. |
| `src/main.cpp` | Documents the demo CLI, input-file loading, command usage, and end-to-end word-count execution flow. |
| `tests/apollo_tests.cpp` | Documents the purpose of each test, the proposal requirements covered, and the expected behavior under normal execution, worker failure, leader failure, speculation, dynamic worker joins, and concurrent jobs. |
| `proto/apollo.proto` | Defines the RPC interface between clients, primaries, workers, and primary replicas. The messages and services describe job submission, job status, job results, worker registration, heartbeats, task execution, and replicated metadata commands. |

This documentation is intended to make the implementation understandable from both the public API and internal distributed-systems design perspectives.

---

## Test Coverage

The `apollo_tests` binary covers the proposal's target scenarios and includes one additional worker-restart test.

| Test | Purpose | Requirements Covered |
|---|---|---|
| `EndToEndWordCountMatchesExpectedOutput` | Verifies basic distributed word-count correctness | MapReduce correctness, task scheduling, result collection |
| `WorkerCrashTriggersTaskReassignment` | Crashes a worker during a running job and verifies reassignment | Worker liveness, task requeue, fault tolerance |
| `LeaderFailurePreservesReplicatedSchedulingState` | Crashes the primary leader while work is in flight | Replicated metadata, leader failover, state recovery |
| `CrashedWorkerCanRestartAndRejoinCluster` | Verifies that a crashed worker can restart and re-register | Dynamic registration, liveness recovery |
| `SpeculativeExecutionIgnoresLateDuplicateResults` | Uses a slow worker to trigger speculative execution | Straggler mitigation, duplicate-result safety |
| `DynamicWorkerJoinHelpsLargeJobFinishCorrectly` | Adds workers after a job has already started | Dynamic scaling, worker joins under load |
| `ConcurrentJobsRemainIsolated` | Submits three jobs concurrently | Multi-job isolation, unique job IDs, metadata isolation |

Run tests with:

```bash
ctest --test-dir build --output-on-failure
```

or simply:

```bash
make test
```

---

## Test Design Rationale

The test suite is designed to cover the major distributed-systems behaviors described in the proposal, not only the happy path.

The tests are reasonable because they exercise the core responsibilities of a MapReduce control plane: job submission, task scheduling, result correctness, worker membership, failure detection, reassignment, leader failover, speculative execution, dynamic scaling, and multi-job isolation.

The tests are fair because the expected word-count output is computed by an independent reference implementation inside the test file. Failure scenarios are injected through the public ApolloCluster API rather than by modifying internal state directly. This keeps the tests close to how a client or test harness would interact with the system.

The tests are complete with respect to the proposal because they cover all proposed scenarios:
- end-to-end MapReduce execution,
- worker crash during execution,
- primary leader failure,
- straggler mitigation,
- dynamic worker join,
- concurrent job submission,
- worker restart/rejoin,
- job-status queries,
- and edge-case input handling.

Together, these tests check both functional correctness and distributed-systems behavior under controlled fault injection.

---

## Documentation Coverage

The project includes both user-facing documentation and code-level documentation.

User-facing documentation is provided in this README and covers:
- system architecture,
- build and test instructions,
- CLI usage,
- configuration options,
- test coverage,
- proposal requirement mapping,
- assumptions,
- and limitations.

Code-level documentation is included in:
- `include/apollo/cluster.h` for the public API,
- `src/cluster.cpp` for the internal runtime, scheduler, worker, and primary logic,
- `src/main.cpp` for the demo CLI,
- `tests/apollo_tests.cpp` for test purpose and requirement coverage.

The documentation is intended to make the design understandable without requiring the reader to reverse-engineer the implementation.

---

## Proposal Requirement Mapping

| Proposal Requirement | Apollo Support |
|---|---|
| R1 Replicated primary state | Simplified Raft-style replicated metadata log |
| R2 Worker registration and liveness tracking | Worker registration RPCs and periodic heartbeats |
| R3 Fault-tolerant task reassignment | Worker failure commands requeue unfinished tasks |
| R4 Deterministic task lifecycle | Explicit task states and replicated task transitions |
| R5 Safe duplicate execution / idempotence | Only one authoritative attempt is committed |
| R6 Straggler mitigation | Speculative duplicate attempts after threshold |
| R7 Dynamic scaling | Workers can join while jobs are running |
| R8 Multi-job isolation | Per-job metadata and outputs |
| R9 Functional MapReduce correctness | Built-in word-count workload tested against reference output |
| R10 Testability and automation | `make all` builds and runs the complete test suite |

---

## Assumptions and Limitations

Apollo is intentionally scoped for a lab setting.

**Current assumptions:**
- All nodes run on localhost
- The test harness runs nodes in one process
- Workers communicate through real gRPC servers and stubs
- The supported workload is built-in word count
- Primary replication is simplified and in-memory
- Failure injection is controlled by the test harness

**Current limitations:**
- No arbitrary user-supplied map/reduce executable or plugin
- No durable log persistence across full process restart
- No production-grade Raft implementation
- No authentication or TLS
- No data locality scheduling
- No distributed filesystem
- No large-scale cluster optimization

---

## Development Notes

The implementation separates the public API from the runtime internals:

- `cluster.h` exposes the user-facing `ApolloCluster` API
- `cluster.cpp` contains the internal primary, worker, scheduler, and replication logic
- `apollo.proto` defines RPC messages and services
- `apollo_tests.cpp` drives the distributed-systems scenarios through the public API

The most important internal path is the **replicated command path**:

```
client/worker/scheduler event
        │
        ▼
replicate command through leader
        │
        ▼
commit after majority
        │
        ▼
apply command to control-plane state
        │
        ▼
scheduler observes updated state
```

This path is used for job submission, worker registration, task assignment, task completion, and worker failure.

---

## Troubleshooting

### `protoc` or `grpc_cpp_plugin` not found

Install Protocol Buffers and gRPC.

**On macOS:**
```bash
brew install protobuf grpc cmake googletest
```

**On Ubuntu/Debian:**
```bash
sudo apt-get update
sudo apt-get install -y build-essential cmake pkg-config protobuf-compiler \
  libprotobuf-dev libgrpc++-dev protobuf-compiler-grpc libgtest-dev
```

### `find_package(gRPC CONFIG REQUIRED)` fails

Make sure gRPC was installed with CMake package configuration files. On macOS with Homebrew, the project adds common Homebrew prefixes automatically in `CMakeLists.txt`.

### Tests timeout

Run with verbose output:

```bash
ctest --test-dir build --output-on-failure -V
```

Also make sure no old `apollo_tests` or `apollo_cli` process is still running.
