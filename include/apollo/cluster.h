#pragma once

#include <chrono>
#include <condition_variable>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace apollo {

/**
 * Identifies the phase of a MapReduce task.
 *
 * A MapReduce job first runs all map tasks. After every map task completes,
 * the cluster creates and schedules reduce tasks.
 */
enum class TaskPhase {
    /** Task reads an input partition and produces intermediate word counts. */
    Map,

    /** Task merges intermediate map outputs for one reduce bucket. */
    Reduce
};

/**
 * Describes the lifecycle state of a logical task.
 *
 * The primary uses these states to coordinate scheduling, retries,
 * speculative duplicates, and failure recovery.
 */
enum class TaskState {
    /** Task has been created but not yet assigned to a worker. */
    Queued,

    /** Task has been selected for a worker but has not fully started yet. */
    Assigned,

    /** Task is currently executing on a worker. */
    Running,

    /** Task finished successfully and its result was committed. */
    Completed,

    /** Task attempt failed or was superseded by another successful attempt. */
    Failed,

    /** Task was returned to the queue after worker failure or leader failover. */
    Requeued
};

/**
 * Describes the lifecycle state of a submitted job.
 */
enum class JobState {
    /** Job has been accepted but has not started running yet. */
    Pending,

    /** Job is actively being scheduled or executed. */
    Running,

    /** Job completed successfully and final output is available. */
    Completed,

    /** Job failed and will not produce a valid final result. */
    Failed
};

/**
 * Runtime configuration for an Apollo cluster.
 *
 * These values control heartbeat frequency, worker failure detection,
 * scheduler polling, speculative execution, and the number of replicated
 * primary nodes used by the control plane.
 */
struct ClusterConfig {
    /**
     * Interval between worker heartbeat RPCs to the current leader.
     */
    std::chrono::milliseconds heartbeat_interval{50};

    /**
     * Maximum time a worker may go without a heartbeat before the primary
     * treats it as failed.
     */
    std::chrono::milliseconds worker_timeout{180};

    /**
     * Interval between scheduler loop iterations.
     *
     * Lower values make task assignment more responsive, while higher values
     * reduce background polling overhead.
     */
    std::chrono::milliseconds scheduler_interval{20};

    /**
     * Time after which a running task may be considered a straggler.
     *
     * Once this threshold is crossed, the primary may launch a speculative
     * duplicate attempt on another available worker.
     */
    std::chrono::milliseconds speculative_threshold{220};

    /**
     * Number of primary replicas in the control-plane cluster.
     *
     * The default value of three allows the replicated metadata log to tolerate
     * one primary failure in the lab setting.
     */
    std::size_t primary_replica_count{3};
};

/**
 * Configuration used when adding a worker to the cluster.
 */
struct WorkerConfig {
    /**
     * Stable worker identifier.
     *
     * If this field is empty, the cluster assigns a generated worker ID.
     */
    std::string worker_id;

    /**
     * Simulated execution speed multiplier for this worker.
     *
     * Larger values make the worker slower. This is mainly used by tests to
     * create stragglers and validate speculative execution.
     */
    double speed_multiplier{1.0};
};

/**
 * Request used to submit a word-count MapReduce job.
 */
struct JobRequest {
    /**
     * Human-readable job name.
     *
     * If empty, the cluster still assigns a unique internal job ID.
     */
    std::string name;

    /**
     * Input text for the word-count workload.
     *
     * The primary partitions this string into map-task inputs.
     */
    std::string input;

    /**
     * Number of map tasks to create for the job.
     */
    int map_tasks{3};

    /**
     * Number of reduce tasks to create after all map tasks complete.
     */
    int reduce_tasks{2};
};

/**
 * Lightweight status summary for a submitted job.
 */
struct JobSummary {
    /** Unique cluster-assigned job identifier. */
    std::string job_id;

    /** Current job lifecycle state. */
    JobState state{JobState::Pending};

    /** Total number of map and reduce tasks known for this job. */
    std::size_t total_tasks{0};

    /** Number of tasks that have completed successfully. */
    std::size_t completed_tasks{0};
};

/**
 * In-process test harness for the Apollo distributed MapReduce cluster.
 *
 * ApolloCluster owns a replicated primary control plane, worker runtimes,
 * scheduler threads, heartbeat loops, and localhost gRPC servers. The class is
 * intended for lab-scale testing of distributed-systems behavior such as worker
 * failure, leader failover, task reassignment, speculative execution, dynamic
 * worker joins, and concurrent job submission.
 *
 * The implementation uses the pImpl pattern so the public header remains small
 * while the runtime details live in the source file.
 */
class ApolloCluster {
public:
    /**
     * Creates a cluster object with the provided runtime configuration.
     *
     * The cluster is not required to start immediately; public operations such
     * as addWorker and submitWordCountJob ensure the runtime is started when
     * needed.
     */
    explicit ApolloCluster(ClusterConfig config = {});

    /**
     * Stops the cluster and releases all owned runtime resources.
     */
    ~ApolloCluster();

    /** ApolloCluster owns threads and servers and cannot be copied. */
    ApolloCluster(const ApolloCluster&) = delete;

    /** ApolloCluster owns threads and servers and cannot be copy-assigned. */
    ApolloCluster& operator=(const ApolloCluster&) = delete;

    /**
     * Starts the primary replicas and background scheduler/consensus loops.
     *
     * Calling start more than once is safe; repeated calls after the first have
     * no effect.
     */
    void start();

    /**
     * Stops all background threads, workers, primary servers, and task RPCs.
     *
     * Calling stop more than once is safe.
     */
    void stop();

    /**
     * Adds a worker to the cluster and starts its local gRPC server.
     *
     * The worker registers with the current leader and becomes eligible for
     * future task assignments.
     *
     * @param config Worker identifier and speed configuration.
     * @return The final worker identifier used by the cluster.
     */
    std::string addWorker(const WorkerConfig& config);

    /**
     * Simulates a worker crash.
     *
     * The worker's server and heartbeat loop are stopped. The primary can then
     * mark the worker unavailable and requeue any unfinished task assigned to it.
     *
     * @param worker_id Identifier of the worker to crash.
     */
    void crashWorker(const std::string& worker_id);

    /**
     * Restarts a previously crashed worker.
     *
     * The worker starts a new local server, registers again with the current
     * leader, and becomes available for later task assignments.
     *
     * @param worker_id Identifier of the worker to restart.
     */
    void restartWorker(const std::string& worker_id);

    /**
     * Updates a worker's simulated execution speed.
     *
     * This is primarily used for tests that create slow workers and validate
     * speculative execution behavior.
     *
     * @param worker_id Identifier of the worker to update.
     * @param speed_multiplier New simulated speed multiplier.
     */
    void setWorkerSpeed(const std::string& worker_id, double speed_multiplier);

    /**
     * Submits a built-in word-count MapReduce job.
     *
     * The primary assigns a unique job ID, partitions the input into map tasks,
     * schedules map and reduce execution, and records task metadata in the
     * replicated control-plane state.
     *
     * @param request Job name, input text, and task counts.
     * @return Unique job identifier assigned by the cluster.
     */
    std::string submitWordCountJob(const JobRequest& request);

    /**
     * Returns a lightweight status summary for a job.
     *
     * @param job_id Unique job identifier returned by submitWordCountJob.
     * @return Current job state and task completion counters.
     */
    JobSummary getJobSummary(const std::string& job_id) const;

    /**
     * Waits for a job to complete and returns its final word-count output.
     *
     * The call polls the current leader until the job completes or the timeout
     * expires.
     *
     * @param job_id Unique job identifier returned by submitWordCountJob.
     * @param timeout Maximum time to wait for job completion.
     * @return Deterministic word-count result sorted by word.
     */
    std::map<std::string, int> waitForJobResult(
        const std::string& job_id,
        std::chrono::milliseconds timeout);

    /**
     * Simulates failure of the current primary leader.
     *
     * A surviving primary replica can be elected as the new leader and continue
     * scheduling from replicated metadata.
     */
    void crashLeader();

    /**
     * Returns the identifier of the current primary leader.
     *
     * @return Current leader ID, or an empty string if no leader is available.
     */
    std::string currentLeaderId() const;

    /**
     * Returns all worker IDs known to the local cluster harness.
     *
     * @return Sorted list of worker identifiers.
     */
    std::vector<std::string> workerIds() const;

private:
    struct WorkerRuntime;
    struct Impl;

    /** Opaque implementation that owns cluster runtime state. */
    std::unique_ptr<Impl> impl_;
};

/**
 * Formats a word-count result map as deterministic text output.
 *
 * Each line has the form:
 *
 *     word count
 *
 * The input map is already ordered lexicographically by word.
 *
 * @param counts Final word-count result.
 * @return Human-readable output string.
 */
std::string formatWordCount(const std::map<std::string, int>& counts);

}  // namespace apollo