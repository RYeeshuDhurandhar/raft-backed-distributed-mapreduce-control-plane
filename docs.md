# Apollo Documentation

Apollo is a lightweight C++17 distributed MapReduce control-plane implementation. It models a small distributed MapReduce cluster using real gRPC services over localhost and a simplified Raft-style replicated metadata log for primary failover in the lab test environment.

Apollo focuses on the core distributed-systems mechanisms required by the project proposal:

- gRPC communication between clients, primaries, and workers
- Replicated primary metadata
- Leader failover
- Worker registration
- Heartbeat-based liveness tracking
- Fault-tolerant task reassignment
- Deterministic task lifecycle management
- Speculative execution for straggler mitigation
- Dynamic worker joins during active jobs
- Concurrent multi-job scheduling with isolated metadata
- Correct MapReduce execution for a built-in word-count workload
- Automated build and test execution through `make all`

---

## Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Repository Layout](#repository-layout)
- [Build and Test](#build-and-test)
- [Demo CLI](#demo-cli)
- [Core Concepts](#core-concepts)
- [Public API](#public-api)
- [Configuration](#configuration)
- [RPC Interface](#rpc-interface)
- [Primary Runtime](#primary-runtime)
- [Worker Runtime](#worker-runtime)
- [Scheduler](#scheduler)
- [Replicated Metadata Log](#replicated-metadata-log)
- [Task Lifecycle](#task-lifecycle)
- [Fault Tolerance](#fault-tolerance)
- [Speculative Execution](#speculative-execution)
- [Dynamic Worker Membership](#dynamic-worker-membership)
- [Multi-Job Isolation](#multi-job-isolation)
- [Word-Count Workload](#word-count-workload)
- [Testing](#testing)
- [Requirement Mapping](#requirement-mapping)
- [Assumptions and Limitations](#assumptions-and-limitations)
- [Troubleshooting](#troubleshooting)

---

## Project Overview

Apollo implements a lab-scale distributed MapReduce framework. A client submits a job to a primary leader. The primary partitions the input into map tasks, schedules those tasks on workers, waits for all map tasks to finish, creates reduce tasks, schedules reduce execution, and finally returns a deterministic result.

The system is organized around three major components:

**1. Client / CLI**

The CLI is the user-facing entry point. It starts a local Apollo cluster, reads an input text file, submits a word-count job, waits for completion, and prints the final result.

**2. Replicated Primary Control Plane**

The primary service manages job submission, worker registration, task assignment, heartbeat tracking, worker failure handling, speculative execution, result collection, and job status queries.

Multiple primary replicas are started in the lab harness. One primary is the current leader. Followers receive replicated metadata commands and can be promoted if the leader fails.

**3. Workers**

Workers register with the primary leader, send periodic heartbeats, and execute map or reduce tasks over gRPC. Workers can crash, restart, or join dynamically while jobs are running.

---

## Architecture

Apollo is designed as an in-process distributed-systems test harness. All nodes run inside one test process, but they communicate using real gRPC servers and stubs over localhost.

```text
+----------------+
| Client / CLI   |
+--------+-------+
         |
         | SubmitJob / GetStatus / GetResult
         v
+---------------------------+
| Primary Leader            |
| - scheduler               |
| - metadata state machine  |
| - worker liveness         |
| - replicated log          |
+------------+--------------+
             |
             | AppendEntries / RequestVote
             v
+---------------------------+
| Primary Followers         |
| - replicated metadata     |
| - election participation  |
+---------------------------+

Primary Leader
      |
      | ExecuteTask RPC
      v
+-------------+     +-------------+     +-------------+
| Worker A    |     | Worker B    |     | Worker C    |
| heartbeats  |     | heartbeats  |     | heartbeats  |
| map/reduce  |     | map/reduce  |     | map/reduce  |
+-------------+     +-------------+     +-------------+
```

The implementation is intentionally scoped for correctness and testability rather than production deployment.

---

## Repository Layout

```
.
├── include/
│   └── apollo/
│       └── cluster.h
├── proto/
│   └── apollo.proto
├── src/
│   ├── cluster.cpp
│   └── main.cpp
├── tests/
│   └── apollo_tests.cpp
├── CMakeLists.txt
├── Makefile
└── README.md
```

| File | Description |
|------|-------------|
| `include/apollo/cluster.h` | Public C++ API for the Apollo cluster. Defines `TaskPhase`, `TaskState`, `JobState`, `ClusterConfig`, `WorkerConfig`, `JobRequest`, `JobSummary`, `ApolloCluster`, and `formatWordCount`. |
| `proto/apollo.proto` | Protocol Buffer and gRPC service definitions. Defines job-submission messages, status messages, result messages, worker-registration messages, heartbeat messages, task-execution messages, replicated metadata log commands, and consensus messages. |
| `src/cluster.cpp` | Main implementation of the Apollo runtime. Contains primary runtime, worker runtime, scheduler loop, consensus loop, task dispatch, heartbeat logic, task lifecycle transitions, replicated metadata command application, and public API forwarding methods. |
| `src/main.cpp` | Demo CLI implementation providing `apollo_cli demo-wordcount <input-file>`. |
| `tests/apollo_tests.cpp` | GoogleTest integration tests for the proposal scenarios. |

---

## Build and Test

Apollo is built with C++17, CMake, Protocol Buffers, gRPC, GoogleTest, CTest, and C++ standard threading primitives.

To build and run everything:

```bash
make all
```

The `make all` target performs:

```bash
make deps
make configure
make build
make test
```

Equivalent manual commands:

```bash
cmake -S . -B build
cmake --build build
ctest --test-dir build --output-on-failure
```

**Cleaning:**

```bash
make clean
```

This removes the generated `build/` directory.

---

## Demo CLI

Run a word-count job on a local input file:

```bash
./build/apollo_cli demo-wordcount path/to/input.txt
```

Example:

```bash
echo "cat dog cat bird dog cat" > input.txt
./build/apollo_cli demo-wordcount input.txt
```

Expected output:

```
job_id=job-1
bird 1
cat 3
dog 2
```

The job ID may vary depending on previous submissions, but the word-count result is deterministic.

---

## Core Concepts

### Task Phase

A task belongs to one of two MapReduce phases:

```cpp
enum class TaskPhase {
    Map,
    Reduce
};
```

- **Map** — receives one input partition and produces intermediate word counts.
- **Reduce** — receives one bucket of intermediate word counts and merges them into a final partial result.

### Task State

Each task follows a deterministic lifecycle:

```cpp
enum class TaskState {
    Queued,
    Assigned,
    Running,
    Completed,
    Failed,
    Requeued
};
```

| State | Description |
|-------|-------------|
| `Queued` | The task exists but has not been assigned to a worker. |
| `Assigned` | The task has been selected for a worker but has not fully started yet. |
| `Running` | The task is currently executing on a worker. |
| `Completed` | The task completed successfully, and its result has been committed. |
| `Failed` | The task attempt failed or was superseded by another successful duplicate attempt. |
| `Requeued` | The logical task was returned to the queue after worker failure or leader failover. |

### Job State

A submitted job has one of four states:

```cpp
enum class JobState {
    Pending,
    Running,
    Completed,
    Failed
};
```

| State | Description |
|-------|-------------|
| `Pending` | The job has been created but is not yet running. |
| `Running` | The job is actively being scheduled or executed. |
| `Completed` | The job has finished successfully and final output is available. |
| `Failed` | The job failed and will not produce a valid result. |

---

## Public API

The main public interface is `ApolloCluster`.

```cpp
class ApolloCluster {
public:
    explicit ApolloCluster(ClusterConfig config = {});
    ~ApolloCluster();

    ApolloCluster(const ApolloCluster&) = delete;
    ApolloCluster& operator=(const ApolloCluster&) = delete;

    void start();
    void stop();

    std::string addWorker(const WorkerConfig& config);
    void crashWorker(const std::string& worker_id);
    void restartWorker(const std::string& worker_id);
    void setWorkerSpeed(const std::string& worker_id, double speed_multiplier);

    std::string submitWordCountJob(const JobRequest& request);
    JobSummary getJobSummary(const std::string& job_id) const;
    std::map<std::string, int> waitForJobResult(
        const std::string& job_id,
        std::chrono::milliseconds timeout);

    void crashLeader();
    std::string currentLeaderId() const;

    std::vector<std::string> workerIds() const;
};
```

### `ApolloCluster`

```cpp
explicit ApolloCluster(ClusterConfig config = {});
```

Creates an Apollo cluster object with the provided configuration. The cluster owns the primary replicas, worker runtimes, scheduler loop, consensus loop, heartbeat loops, and task-dispatch threads. The runtime may be started explicitly with `start()` or implicitly by methods such as `addWorker()` and `submitWordCountJob()`.

| Parameter | Description |
|-----------|-------------|
| `config` | Runtime configuration for heartbeats, timeouts, scheduler frequency, speculative threshold, and primary replica count. |

### `~ApolloCluster`

```cpp
~ApolloCluster();
```

Destroys the cluster and releases all owned resources. Stops background threads, gRPC servers, worker runtimes, and task-dispatch threads through the private implementation cleanup path.

### `start`

```cpp
void start();
```

Starts the Apollo runtime. Creates primary replicas, starts primary gRPC servers, selects the first primary as the initial leader, starts the scheduler loop, and starts the consensus loop. Calling `start()` more than once is safe.

### `stop`

```cpp
void stop();
```

Stops the Apollo runtime. Signals shutdown, joins scheduler and consensus threads, stops worker heartbeat loops, shuts down worker gRPC servers, joins in-flight task RPC threads, shuts down primary gRPC servers, and clears runtime state. Calling `stop()` more than once is safe.

### `addWorker`

```cpp
std::string addWorker(const WorkerConfig& config);
```

Adds a worker to the cluster. The worker starts a localhost gRPC server and a heartbeat thread, registers with the current primary leader, and becomes eligible for future task assignments.

| Parameter | Description |
|-----------|-------------|
| `config` | Worker ID and speed multiplier. |

Returns the final worker ID used by the cluster. If `config.worker_id` is empty, Apollo generates a worker ID.

### `crashWorker`

```cpp
void crashWorker(const std::string& worker_id);
```

Simulates a worker crash. The worker runtime is stopped, its gRPC server is shut down, and the failure is recorded through the replicated metadata path. If the worker was running a task, the task is requeued and can be assigned to another healthy worker.

### `restartWorker`

```cpp
void restartWorker(const std::string& worker_id);
```

Restarts a previously crashed worker. The worker starts a new gRPC server and heartbeat thread, then re-registers with the current primary leader.

### `setWorkerSpeed`

```cpp
void setWorkerSpeed(const std::string& worker_id, double speed_multiplier);
```

Updates a worker's simulated execution speed. A larger `speed_multiplier` makes the worker slower. Useful for creating stragglers in speculative execution tests.

### `submitWordCountJob`

```cpp
std::string submitWordCountJob(const JobRequest& request);
```

Submits a built-in word-count MapReduce job. The current leader assigns a unique job ID, records the job in replicated metadata, partitions the input into map tasks, and schedules work on available workers.

Returns the unique job ID assigned by Apollo. Throws `std::runtime_error` if no leader accepts the job within the retry window.

### `getJobSummary`

```cpp
JobSummary getJobSummary(const std::string& job_id) const;
```

Returns current job status as a `JobSummary` containing the job ID, job state, total number of tasks, and completed task count. Throws `std::runtime_error` if no leader is available or status lookup fails.

### `waitForJobResult`

```cpp
std::map<std::string, int> waitForJobResult(
    const std::string& job_id,
    std::chrono::milliseconds timeout);
```

Waits for a job to complete and returns the final word-count output. Repeatedly queries the current leader until the job reaches `Completed` or the timeout expires. Returns a sorted map from word to count. Throws `std::runtime_error` if the timeout expires.

### `crashLeader`

```cpp
void crashLeader();
```

Simulates failure of the current primary leader. A surviving primary can later win an election and continue scheduling from replicated metadata.

### `currentLeaderId`

```cpp
std::string currentLeaderId() const;
```

Returns the ID of the current primary leader, or an empty string if no leader is available.

### `workerIds`

```cpp
std::vector<std::string> workerIds() const;
```

Returns a sorted list of all worker IDs known to the local cluster harness.

### `formatWordCount`

```cpp
std::string formatWordCount(const std::map<std::string, int>& counts);
```

Formats a word-count result map as deterministic line-oriented text. Each line has the form `word count`.

---

## Configuration

### `ClusterConfig`

```cpp
struct ClusterConfig {
    std::chrono::milliseconds heartbeat_interval{50};
    std::chrono::milliseconds worker_timeout{180};
    std::chrono::milliseconds scheduler_interval{20};
    std::chrono::milliseconds speculative_threshold{220};
    std::size_t primary_replica_count{3};
};
```

| Field | Default | Description |
|-------|---------|-------------|
| `heartbeat_interval` | 50 ms | Interval between worker heartbeat RPCs. |
| `worker_timeout` | 180 ms | Time after which a missing heartbeat marks a worker failed. |
| `scheduler_interval` | 20 ms | Interval between scheduler loop iterations. |
| `speculative_threshold` | 220 ms | Time after which a running task may be considered a straggler. |
| `primary_replica_count` | 3 | Number of primary replicas. |

Example:

```cpp
apollo::ApolloCluster cluster({
    std::chrono::milliseconds(40),
    std::chrono::milliseconds(220),
    std::chrono::milliseconds(20),
    std::chrono::milliseconds(150),
    3,
});
```

### `WorkerConfig`

```cpp
struct WorkerConfig {
    std::string worker_id;
    double speed_multiplier{1.0};
};
```

| Field | Description |
|-------|-------------|
| `worker_id` | Stable worker identifier. |
| `speed_multiplier` | Simulated task execution speed multiplier. A larger multiplier means slower execution. |

Example:

```cpp
cluster.addWorker({"worker-fast", 1.0});
cluster.addWorker({"worker-slow", 5.0});
```

### `JobRequest`

```cpp
struct JobRequest {
    std::string name;
    std::string input;
    int map_tasks{3};
    int reduce_tasks{2};
};
```

| Field | Description |
|-------|-------------|
| `name` | Human-readable job name. |
| `input` | Raw input text. |
| `map_tasks` | Number of map tasks. |
| `reduce_tasks` | Number of reduce tasks. |

Example:

```cpp
const auto job_id = cluster.submitWordCountJob({
    "wordcount",
    "cat dog cat bird dog cat",
    3,
    2,
});
```

### `JobSummary`

```cpp
struct JobSummary {
    std::string job_id;
    JobState state{JobState::Pending};
    std::size_t total_tasks{0};
    std::size_t completed_tasks{0};
};
```

| Field | Description |
|-------|-------------|
| `job_id` | Unique job identifier. |
| `state` | Current job state. |
| `total_tasks` | Total number of known map and reduce tasks. |
| `completed_tasks` | Number of completed tasks. |

---

## RPC Interface

Apollo uses Protocol Buffers and gRPC. The protobuf package is `apollo.rpc`.

Apollo defines three services:

```protobuf
service ApolloPrimary { ... }
service ApolloWorker { ... }
service ApolloConsensus { ... }
```

### `ApolloPrimary`

Handles client and worker control-plane RPCs.

```protobuf
service ApolloPrimary {
  rpc SubmitJob(SubmitJobRequest) returns (SubmitJobReply);
  rpc GetJobStatus(JobStatusRequest) returns (JobStatusReply);
  rpc GetJobResult(JobResultRequest) returns (JobResultReply);
  rpc RegisterWorker(RegisterWorkerRequest) returns (RegisterWorkerReply);
  rpc Heartbeat(HeartbeatRequest) returns (HeartbeatReply);
}
```

**SubmitJob** — submits a job to the primary leader. If the receiving primary is not the leader, it rejects the request and returns a leader hint.

```protobuf
message SubmitJobRequest {
  string name = 1;
  string input = 2;
  int32 map_tasks = 3;
  int32 reduce_tasks = 4;
}
message SubmitJobReply {
  string job_id = 1;
  bool accepted = 2;
  string leader_hint = 3;
}
```

**GetJobStatus** — returns job status.

```protobuf
message JobStatusRequest { string job_id = 1; }
message JobStatusReply {
  string job_id = 1;
  JobState state = 2;
  uint32 total_tasks = 3;
  uint32 completed_tasks = 4;
  string leader_hint = 5;
}
```

**GetJobResult** — returns final result data.

```protobuf
message JobResultRequest { string job_id = 1; }
message JobResultReply {
  string job_id = 1;
  JobState state = 2;
  repeated WordCount counts = 3;
  string leader_hint = 4;
}
```

**RegisterWorker** — registers a worker with the primary leader.

```protobuf
message RegisterWorkerRequest {
  string worker_id = 1;
  string worker_address = 2;
  double speed_multiplier = 3;
}
message RegisterWorkerReply {
  bool accepted = 1;
  string leader_hint = 2;
}
```

**Heartbeat** — refreshes worker liveness.

```protobuf
message HeartbeatRequest { string worker_id = 1; }
message HeartbeatReply {
  bool accepted = 1;
  string leader_hint = 2;
}
```

### `ApolloWorker`

Executes tasks.

```protobuf
service ApolloWorker {
  rpc ExecuteTask(ExecuteTaskRequest) returns (ExecuteTaskReply);
}

message ExecuteTaskRequest {
  string job_id = 1;
  TaskPhase phase = 2;
  int32 task_index = 3;
  int32 attempt_id = 4;
  int32 reducers = 5;
  string input_partition = 6;
  repeated WordCount map_outputs = 7;
}
message ExecuteTaskReply {
  bool success = 1;
  repeated WordCount counts = 2;
}
```

For map tasks, `input_partition` is populated. For reduce tasks, `map_outputs` contains intermediate counts assigned to that reduce bucket.

### `ApolloConsensus`

Supports simplified leader election and log replication.

```protobuf
service ApolloConsensus {
  rpc RequestVote(RequestVoteRequest) returns (RequestVoteReply);
  rpc AppendEntries(AppendEntriesRequest) returns (AppendEntriesReply);
}
```

**RequestVote** — used by a candidate primary to request votes.

```protobuf
message RequestVoteRequest {
  int64 term = 1;
  string candidate_id = 2;
  int64 last_log_index = 3;
  int64 last_log_term = 4;
}
message RequestVoteReply {
  int64 term = 1;
  bool vote_granted = 2;
}
```

**AppendEntries** — used by the leader to replicate log entries and send heartbeats.

```protobuf
message AppendEntriesRequest {
  int64 term = 1;
  string leader_id = 2;
  int64 leader_commit = 3;
  repeated LogCommand entries = 4;
}
message AppendEntriesReply {
  int64 term = 1;
  bool success = 2;
  int64 match_index = 3;
}
```

---

## Primary Runtime

Each primary replica is represented internally by `PrimaryRuntime`:

```cpp
struct PrimaryRuntime {
    std::string node_id;
    std::string address;
    bool alive{true};
    bool leader{false};
    std::int64_t current_term{1};
    std::string voted_for;
    std::vector<rpc::LogCommand> log;
    std::int64_t commit_index{0};
    std::int64_t last_applied{0};
    TimePoint last_leader_contact{Clock::now()};
    std::string leader_hint;
    ControlPlaneState state;
    std::unique_ptr<grpc::Server> server;
    std::thread server_thread;
};
```

A primary replica hosts `ApolloPrimary` and `ApolloConsensus` RPCs, stores a replicated metadata log, applies committed commands to its state machine, participates in leader election, and may become leader after failure of the old leader. Only the leader accepts mutating client/worker requests.

### Control-Plane State

```cpp
struct ControlPlaneState {
    int next_job_id{1};
    std::unordered_map<std::string, WorkerRecord> workers;
    std::unordered_map<std::string, ActiveTask> active_tasks_by_worker;
    std::unordered_map<std::string, JobRecord> jobs;
};
```

| Field | Description |
|-------|-------------|
| `next_job_id` | Counter used to assign unique job IDs. |
| `workers` | Worker membership and liveness metadata. |
| `active_tasks_by_worker` | Current task attempt assigned to each worker. |
| `jobs` | Metadata for all submitted jobs. |

All important changes to this state are applied through replicated log commands.

---

## Worker Runtime

Each worker is represented internally by `WorkerRuntime`:

```cpp
struct WorkerRuntime {
    WorkerConfig config;
    std::string address;
    std::unique_ptr<grpc::Server> server;
    std::thread server_thread;
    std::thread heartbeat_thread;
    std::unique_ptr<WorkerServiceImpl> service;
    std::atomic<bool> stop_requested{false};
    std::atomic<bool> live_server{true};
};
```

A worker starts a gRPC server, registers with the current leader, sends periodic heartbeats, executes map and reduce tasks, and can be crashed or restarted by tests.

---

## Scheduler

The scheduler runs in a background thread. Its main loop:

```
while not shutdown:
    find current leader
    collect timed-out workers
    plan speculative attempts
    plan queued task assignments
    replicate worker-failure commands
    replicate task-assignment commands
    dispatch committed task attempts
    sleep scheduler_interval
```

The scheduler only runs meaningful scheduling work on the current leader.

**Assignment Rules** — a task can be assigned if the job is running, the task is `Queued` or `Requeued`, a worker is registered and alive and available, the worker is not already running another attempt of the same task, and the worker has not already been reserved in the current scheduler cycle. Map tasks are scheduled before reduce tasks. Reduce tasks are scheduled only after every map task in the job completes.

---

## Replicated Metadata Log

Apollo uses a simplified Raft-style log of metadata commands:

```protobuf
message LogCommand {
  int64 index = 1;
  int64 term = 2;
  oneof payload {
    RegisterWorkerCommand register_worker = 3;
    SubmitJobCommand submit_job = 4;
    AssignTaskCommand assign_task = 5;
    CompleteTaskCommand complete_task = 6;
    WorkerFailedCommand worker_failed = 7;
  }
}
```

| Command | Description |
|---------|-------------|
| `RegisterWorkerCommand` | Records worker membership (`worker_id`, `worker_address`, `speed_multiplier`). |
| `SubmitJobCommand` | Records a submitted job (`job_id`, `name`, `input`, `map_tasks`, `reduce_tasks`). |
| `AssignTaskCommand` | Records task assignment (`job_id`, `phase`, `task_index`, `attempt_id`, `worker_id`, `speculative`). |
| `CompleteTaskCommand` | Records task completion with output counts. |
| `WorkerFailedCommand` | Records worker failure (`worker_id`). |

### Replication Path

```
event occurs
    → leader creates LogCommand
    → leader appends command locally
    → leader sends AppendEntries to followers
    → majority accepts
    → leader advances commit_index
    → leader applies committed command
    → followers receive commit index through heartbeat
    → followers apply committed command
```

This path is used for worker registration, job submission, task assignment, task completion, and worker failure.

---

## Task Lifecycle

Each logical task is represented by `TaskRecord`:

```cpp
struct TaskRecord {
    int task_index{0};
    TaskPhase phase{TaskPhase::Map};
    TaskState state{TaskState::Queued};
    bool speculative_launched{false};
    int authoritative_attempt_id{-1};
    std::vector<AttemptRecord> attempts;
    std::unordered_map<std::string, int> output;
};
```

Each physical attempt is represented by `AttemptRecord`:

```cpp
struct AttemptRecord {
    int attempt_id{0};
    std::string worker_id;
    bool speculative{false};
    TaskState state{TaskState::Assigned};
    TimePoint started_at{};
};
```

### Normal Lifecycle

```
Queued → [AssignTaskCommand] → Running → [CompleteTaskCommand] → Completed
```

### Worker-Failure Lifecycle

```
Queued → Running → [WorkerFailedCommand] → Requeued → Running → Completed
```

### Speculative Lifecycle

```
Running attempt A
    → slow task detected
    → Running attempt A + Running attempt B
    → first successful completion → Completed
    → late duplicate completion → ignored / marked Failed
```

Only one attempt is authoritative.

---

## Fault Tolerance

### Worker Failure

Workers send periodic heartbeats to the primary leader. If a worker misses heartbeats for longer than `worker_timeout`, the scheduler marks it failed by replicating a `WorkerFailedCommand`. When that command is applied, the worker is marked not alive and unavailable, any active task assigned to that worker is marked failed, the logical task is requeued, and the active assignment is removed. Later, the scheduler assigns the requeued task to another healthy worker.

### Leader Failure

```cpp
cluster.crashLeader();
```

After leader failure, surviving primaries stop receiving leader heartbeats, a follower starts an election, the follower requests votes, and if it receives a majority it becomes leader. In-flight tasks are conservatively requeued, completed tasks remain completed, and scheduling continues from replicated metadata. This allows a job to continue after primary leader failure without losing committed metadata.

---

## Speculative Execution

A task becomes eligible for speculation if it is currently running, has not already launched a speculative copy, has at least one active attempt, its latest attempt has exceeded `speculative_threshold`, and another healthy worker is available.

When eligible, the scheduler creates another `AssignTaskCommand` for the same logical task with a new attempt ID and a different worker. The first successful completion is committed; later completions for the same logical task are ignored. This ensures correctness even when duplicate attempts execute concurrently.

---

## Dynamic Worker Membership

Workers can join while jobs are already running. When `addWorker()` is called, a worker runtime is created, a worker gRPC server starts, a heartbeat thread starts, the worker registers with the current leader, the registration is replicated, and the worker becomes eligible for future assignments. This supports dynamic scaling without cluster restart.

---

## Multi-Job Isolation

Apollo supports concurrent job submission. Each job has independent job ID, input partitions, map tasks, reduce tasks, task attempts, intermediate outputs, and final output. Jobs are stored in:

```cpp
std::unordered_map<std::string, JobRecord> jobs;
```

This prevents metadata or intermediate data from one job from interfering with another job.

---

## Word-Count Workload

Apollo currently supports a built-in word-count workload.

**Tokenization Rules:** alphanumeric characters form words, non-alphanumeric characters split words, and words are converted to lowercase.

Example input: `Cat dog, cat! BIRD dog cat.` → tokenized as: `cat`, `dog`, `cat`, `bird`, `dog`, `cat` → output: `bird 1`, `cat 3`, `dog 2`.

**Map Phase** — each map task receives one partition of the input and computes local word counts.

**Reduce Phase** — after all map tasks complete, reduce tasks are created. Words are bucketed using `std::hash<std::string>{}(word) % reducers`. Each reduce task merges counts for its bucket.

**Final Output** — a sorted `std::map<std::string, int>`. `formatWordCount()` prints it as `word count` per line.

---

## Testing

Apollo uses GoogleTest. Run tests with:

```bash
make test
# or
ctest --test-dir build --output-on-failure
```

### Test: `EndToEndWordCountMatchesExpectedOutput`

Validates basic distributed MapReduce correctness. Starts cluster, adds three workers, submits a word-count job, waits for completion, and compares output with a local reference implementation. Covers functional MapReduce correctness, task scheduling, and result collection.

### Test: `WorkerCrashTriggersTaskReassignment`

Validates worker failure handling. Starts cluster, adds three workers, submits a long-running job, crashes one worker during execution, and verifies the job still completes correctly. Covers worker liveness tracking, failure detection, task requeue, and task reassignment.

### Test: `LeaderFailurePreservesReplicatedSchedulingState`

Validates leader failover. Starts replicated primary cluster, adds workers, submits a job, crashes the current leader, waits for a new leader, and verifies the job completes correctly. Covers replicated primary metadata, leader election, state recovery, and continued scheduling after failover.

### Test: `CrashedWorkerCanRestartAndRejoinCluster`

Validates worker restart. Starts cluster, adds two workers, crashes one, restarts it, submits a job, and verifies the job completes correctly. Covers worker restart, dynamic registration, and liveness recovery.

### Test: `SpeculativeExecutionIgnoresLateDuplicateResults`

Validates speculative execution. Starts cluster with a shorter speculative threshold, adds one slow worker and two normal workers, submits a job, and verifies final output is correct despite possible duplicate attempts. Covers straggler mitigation, speculative execution, duplicate-result safety, and idempotent task completion.

### Test: `DynamicWorkerJoinHelpsLargeJobFinishCorrectly`

Validates dynamic worker joins. Starts cluster with one worker, submits a large job, adds three more workers while the job is running, and verifies the job completes correctly. Covers dynamic scaling, worker registration under load, and scheduling with changing membership.

### Test: `ConcurrentJobsRemainIsolated`

Validates concurrent job isolation. Starts cluster, adds four workers, submits three jobs concurrently, and verifies each job receives a unique job ID and its own correct output. Covers concurrent job submission, unique job IDs, isolated job metadata, and isolated final outputs.

---

## Requirement Mapping

| Proposal Requirement | Apollo Implementation |
|---------------------|----------------------|
| R1 Replicated primary state | Simplified Raft-style replicated metadata command log |
| R2 Worker registration and liveness tracking | Worker registration RPCs and heartbeat RPCs |
| R3 Fault-tolerant task reassignment | Worker failure command requeues unfinished active tasks |
| R4 Deterministic task lifecycle | Explicit task states and log-driven transitions |
| R5 Safe duplicate execution / idempotence | Only one authoritative task attempt is committed |
| R6 Straggler mitigation | Speculative duplicate attempts after threshold |
| R7 Dynamic scaling | Workers can join while jobs are running |
| R8 Multi-job isolation | Per-job metadata, attempts, outputs, and final results |
| R9 Functional MapReduce correctness | Built-in word-count workload tested against reference implementation |
| R10 Testability and automation | `make all` configures, builds, and runs tests |

---

## Assumptions and Limitations

**Assumptions:**
- All nodes run on localhost.
- The cluster is small.
- The test harness controls failures.
- Workers run in the same process as the test but communicate over real gRPC.
- The supported workload is word count.
- The replicated metadata log is in memory.
- Primary failure means simulated primary-server failure, not full machine crash.

**Limitations:**
- No distributed filesystem.
- No arbitrary user-provided map/reduce executable.
- No persistent metadata across full process restart.
- No production-grade Raft implementation (no snapshotting, no durable log storage).
- No authentication or TLS.
- No data-locality-aware scheduling.
- No large-cluster optimization or production deployment support.

---

## Troubleshooting

**`protoc` Not Found**

```bash
# macOS
brew install protobuf

# Ubuntu/Debian
sudo apt-get update && sudo apt-get install -y protobuf-compiler libprotobuf-dev
```

**`grpc_cpp_plugin` Not Found**

```bash
# macOS
brew install grpc

# Ubuntu/Debian
sudo apt-get install -y libgrpc++-dev protobuf-compiler-grpc
```

**CMake Cannot Find gRPC**

Make sure gRPC was installed with CMake package configuration files. On macOS with Homebrew:

```bash
cmake -S . -B build -DCMAKE_PREFIX_PATH=/opt/homebrew
```

**GoogleTest Not Found**

The CMake configuration attempts to find GoogleTest locally; if not found, it fetches it automatically via FetchContent. To install manually:

```bash
# macOS
brew install googletest

# Ubuntu/Debian
sudo apt-get install -y libgtest-dev
```

**Tests Timeout**

```bash
ctest --test-dir build --output-on-failure -V
```

Check that old test or CLI processes are not still running.

**Port Binding Issues**

Apollo binds gRPC servers to ephemeral localhost ports using port zero. If binding fails, check whether the environment restricts localhost networking.

---

## Example End-to-End Usage

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
        "demo",
        "cat dog cat bird dog cat",
        3,
        2,
    });

    const auto result = cluster.waitForJobResult(job_id, std::chrono::seconds(5));

    std::cout << "job_id=" << job_id << "\n";
    std::cout << apollo::formatWordCount(result);

    return 0;
}
```

Output:

```
job_id=job-1
bird 1
cat 3
dog 2
```

---

## Summary

Apollo demonstrates the main distributed-systems behaviors required by the lab proposal: job submission, distributed task execution, replicated scheduling metadata, primary leader failover, worker registration, heartbeat-based failure detection, task reassignment, speculative execution, dynamic worker joins, concurrent job isolation, and deterministic word-count correctness. It is intentionally lightweight and lab-focused, while still using real gRPC communication and realistic control-plane mechanisms.
