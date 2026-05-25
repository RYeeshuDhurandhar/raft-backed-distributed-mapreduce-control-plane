#include "apollo/cluster.h"

#include "apollo.grpc.pb.h"

#include <grpcpp/create_channel.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

namespace apollo {

namespace {

/**
 * Internal implementation namespace for Apollo.
 *
 * This file contains the complete in-process runtime for the Apollo lab
 * cluster. Although the public API is exposed through ApolloCluster, the
 * implementation internally starts multiple localhost gRPC servers to model
 * a distributed MapReduce deployment:
 *
 * - primary replicas that maintain replicated scheduling metadata,
 * - workers that execute map and reduce tasks,
 * - background heartbeat loops for liveness tracking,
 * - a scheduler loop for task assignment and speculative execution,
 * - and a lightweight Raft-style replicated command log for leader failover.
 *
 * The runtime is intentionally scoped for deterministic lab testing rather
 * than production deployment. The focus is correctness under worker failure,
 * leader failure, retries, duplicate execution, and concurrent jobs.
 */

using Clock = std::chrono::steady_clock;
using TimePoint = Clock::time_point;
namespace rpc = apollo::rpc;

/**
 * Returns a wildcard localhost bind address using port zero.
 *
 * Port zero lets gRPC ask the operating system for an available ephemeral
 * port. The selected port is later converted into a concrete 127.0.0.1
 * address for client stubs.
 */
std::string localhostWildcardAddress() {
    return "0.0.0.0:0";
}

/**
 * Builds a concrete loopback address for a selected server port.
 *
 * @param port Ephemeral port selected by gRPC.
 * @return Address string usable by gRPC clients.
 */
std::string localhostAddress(int port) {
    return "127.0.0.1:" + std::to_string(port);
}

/**
 * Converts a protobuf task phase into the public Apollo task phase enum.
 */
TaskPhase fromProtoPhase(rpc::TaskPhase phase) {
    return phase == rpc::TASK_PHASE_REDUCE ? TaskPhase::Reduce : TaskPhase::Map;
}

/**
 * Converts an Apollo task phase into its protobuf representation.
 */
rpc::TaskPhase toProtoPhase(TaskPhase phase) {
    return phase == TaskPhase::Reduce ? rpc::TASK_PHASE_REDUCE : rpc::TASK_PHASE_MAP;
}

/**
 * Converts a protobuf job state into the public Apollo job state enum.
 */
JobState fromProtoJobState(rpc::JobState state) {
    switch (state) {
        case rpc::JOB_STATE_COMPLETED:
            return JobState::Completed;
        case rpc::JOB_STATE_FAILED:
            return JobState::Failed;
        case rpc::JOB_STATE_RUNNING:
            return JobState::Running;
        case rpc::JOB_STATE_PENDING:
        default:
            return JobState::Pending;
    }
}

/**
 * Converts an Apollo job state into its protobuf representation.
 */
rpc::JobState toProtoJobState(JobState state) {
    switch (state) {
        case JobState::Completed:
            return rpc::JOB_STATE_COMPLETED;
        case JobState::Failed:
            return rpc::JOB_STATE_FAILED;
        case JobState::Running:
            return rpc::JOB_STATE_RUNNING;
        case JobState::Pending:
        default:
            return rpc::JOB_STATE_PENDING;
    }
}

/**
 * Tokenizes and partitions a job input string into map-task inputs.
 *
 * The tokenizer treats alphanumeric runs as words, lowercases them, and
 * distributes words round-robin across the requested number of partitions.
 * This gives deterministic partitioning for the built-in word-count workload.
 *
 * @param input Raw input text submitted by the client.
 * @param partitions Requested number of map partitions.
 * @return One input string per map task.
 */
std::vector<std::string> partitionInput(const std::string& input, int partitions) {
    std::vector<std::string> tokens;
    std::string current;
    for (char ch : input) {
        if (std::isalnum(static_cast<unsigned char>(ch))) {
            current.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(ch))));
        } else if (!current.empty()) {
            tokens.push_back(current);
            current.clear();
        }
    }
    if (!current.empty()) {
        tokens.push_back(current);
    }

    partitions = std::max(1, partitions);
    std::vector<std::string> result(static_cast<std::size_t>(partitions));
    for (std::size_t i = 0; i < tokens.size(); ++i) {
        auto idx = i % result.size();
        if (!result[idx].empty()) {
            result[idx].push_back(' ');
        }
        result[idx] += tokens[i];
    }
    if (result.empty()) {
        result.push_back("");
    }
    return result;
}

/**
 * Computes case-insensitive word counts for a text fragment.
 *
 * This function is used by map tasks. It mirrors the same tokenization logic
 * used during input partitioning so tests can compare deterministic results.
 *
 * @param input Text fragment assigned to one map task.
 * @return Word-count map for the fragment.
 */
std::unordered_map<std::string, int> countWords(const std::string& input) {
    std::unordered_map<std::string, int> counts;
    std::string current;
    for (char ch : input) {
        if (std::isalnum(static_cast<unsigned char>(ch))) {
            current.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(ch))));
        } else if (!current.empty()) {
            ++counts[current];
            current.clear();
        }
    }
    if (!current.empty()) {
        ++counts[current];
    }
    return counts;
}

/**
 * Serializes word-count data into a protobuf repeated WordCount field.
 *
 * Counts are sorted by word before serialization to keep RPC payloads and
 * output ordering deterministic.
 *
 * @param counts In-memory word-count table.
 * @param out Destination protobuf repeated field.
 */
void fillCounts(const std::unordered_map<std::string, int>& counts, google::protobuf::RepeatedPtrField<rpc::WordCount>* out) {
    std::vector<std::pair<std::string, int>> sorted(counts.begin(), counts.end());
    std::sort(sorted.begin(), sorted.end());
    for (const auto& [word, count] : sorted) {
        auto* wc = out->Add();
        wc->set_word(word);
        wc->set_count(count);
    }
}

/**
 * Deserializes protobuf WordCount entries into an unordered count table.
 *
 * Repeated entries for the same word are merged by summing their counts.
 *
 * @param in Protobuf repeated WordCount field.
 * @return In-memory word-count table.
 */
std::unordered_map<std::string, int> readCounts(const google::protobuf::RepeatedPtrField<rpc::WordCount>& in) {
    std::unordered_map<std::string, int> counts;
    for (const auto& wc : in) {
        counts[wc.word()] += wc.count();
    }
    return counts;
}

/**
 * Metadata for one physical execution attempt of a logical task.
 *
 * A logical task may have multiple attempts because of retries, worker
 * failures, leader failover, or speculative execution. Only one attempt is
 * eventually committed as authoritative.
 */
struct AttemptRecord {
    int attempt_id{0};
    std::string worker_id;
    bool speculative{false};
    TaskState state{TaskState::Assigned};
    TimePoint started_at{};
};

/**
 * Replicated metadata for one logical map or reduce task.
 *
 * The primary uses this record to track task state, all execution attempts,
 * speculative launch status, committed output, and the authoritative attempt
 * that completed the task.
 */
struct TaskRecord {
    int task_index{0};
    TaskPhase phase{TaskPhase::Map};
    TaskState state{TaskState::Queued};
    bool speculative_launched{false};
    int authoritative_attempt_id{-1};
    std::vector<AttemptRecord> attempts;
    std::unordered_map<std::string, int> output;
};

/**
 * Replicated metadata for one submitted MapReduce job.
 *
 * A job owns its map tasks, reduce tasks, input partitions, and final output.
 * Keeping this metadata per job ensures concurrent jobs remain isolated from
 * each other.
 */
struct JobRecord {
    std::string job_id;
    std::string name;
    JobState state{JobState::Pending};
    int reducers{1};
    std::vector<std::string> input_partitions;
    std::vector<TaskRecord> map_tasks;
    std::vector<TaskRecord> reduce_tasks;
    std::map<std::string, int> final_output;
};

/**
 * Replicated control-plane metadata for one worker.
 *
 * The primary tracks registration status, liveness, scheduling availability,
 * worker address, heartbeat time, and simulated speed for straggler tests.
 */
struct WorkerRecord {
    std::string worker_id;
    std::string address;
    bool registered{false};
    bool alive{true};
    bool available{true};
    double speed_multiplier{1.0};
    TimePoint last_heartbeat{};
};

/**
 * Records the task attempt currently assigned to a worker.
 *
 * This map is used to release workers safely, detect worker-failure impact,
 * and avoid incorrectly freeing a worker after a stale duplicate attempt.
 */
struct ActiveTask {
    std::string job_id;
    TaskPhase phase{TaskPhase::Map};
    int task_index{0};
    int attempt_id{0};
};

/**
 * Replicated scheduling state maintained by each primary replica.
 *
 * All state transitions that affect jobs, workers, assignments, completions,
 * and failures are applied through replicated log commands. A new leader can
 * therefore continue scheduling from the committed metadata state.
 */
struct ControlPlaneState {
    int next_job_id{1};
    std::unordered_map<std::string, WorkerRecord> workers;
    std::unordered_map<std::string, ActiveTask> active_tasks_by_worker;
    std::unordered_map<std::string, JobRecord> jobs;
};

/**
 * Runtime state for one primary replica.
 *
 * Each primary owns a gRPC server and a local copy of the replicated command
 * log. The leader handles client and worker requests, while followers receive
 * AppendEntries and may later be promoted through the simplified election path.
 */
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

}  // namespace

/**
 * Private implementation of ApolloCluster.
 *
 * Impl owns the full lab runtime: primary replicas, workers, gRPC services,
 * scheduler thread, consensus thread, and task RPC threads. The public
 * ApolloCluster class delegates to this implementation through the pImpl
 * pattern so the external API remains compact.
 */
struct ApolloCluster::Impl {
    struct PrimaryServiceImpl;
    struct ConsensusServiceImpl;
    struct WorkerServiceImpl;
    struct WorkerRuntime;

    /**
     * Scheduler decision that has not yet been committed.
     *
     * The scheduler first computes planned assignments under the mutex, then
     * releases the mutex and replicates each assignment through the command log.
     * This avoids performing blocking RPC replication while holding scheduler
     * state longer than necessary.
     */
    struct PlannedAssignment {
        std::string leader_id;
        std::string job_id;
        std::string worker_id;
        TaskPhase phase{TaskPhase::Map};
        int task_index{0};
        int attempt_id{0};
        bool speculative{false};
    };

    explicit Impl(ClusterConfig cfg)
        : config(std::move(cfg)) {}

    ~Impl() {
        stop();
    }

    ClusterConfig config;
    mutable std::mutex mutex;
    std::condition_variable cv;
    bool started{false};
    std::atomic<bool> shutdown{false};
    std::thread scheduler_thread;
    std::thread consensus_thread;
    std::vector<std::thread> task_threads;
    std::unordered_map<std::string, std::unique_ptr<PrimaryRuntime>> primaries;
    std::vector<std::string> primary_order;
    std::unordered_map<std::string, std::unique_ptr<PrimaryServiceImpl>> primary_services;
    std::unordered_map<std::string, std::unique_ptr<ConsensusServiceImpl>> consensus_services;
    std::unordered_map<std::string, std::unique_ptr<WorkerRuntime>> workers;
    
    /**
     * Local runtime for one simulated worker process.
     *
     * In the lab harness, each worker runs inside the same process as the tests,
     * but exposes a real localhost gRPC server. This allows tests to exercise RPC
     * behavior without requiring multiple operating-system processes.
     */
    struct WorkerRuntime {
        WorkerConfig config;
        std::string address;
        std::unique_ptr<grpc::Server> server;
        std::thread server_thread;
        std::thread heartbeat_thread;
        std::unique_ptr<WorkerServiceImpl> service;
        std::atomic<bool> stop_requested{false};
        std::atomic<bool> live_server{true};

        WorkerRuntime(WorkerConfig cfg, std::string addr)
            : config(std::move(cfg)), address(std::move(addr)) {}
    };

    /**
     * gRPC service exposed by primary replicas.
     *
     * Client-facing and worker-facing control-plane RPCs arrive here. Only the
     * current leader accepts mutating requests such as job submission and worker
     * registration. Followers return a leader hint so callers can retry against
     * the active leader.
     */
    struct PrimaryServiceImpl final : rpc::ApolloPrimary::Service {
        explicit PrimaryServiceImpl(Impl& owner, std::string id)
            : impl(owner), node_id(std::move(id)) {}

    /**
     * Handles client job submission on the current leader.
     *
     * The leader reserves a unique job ID, appends a SubmitJobCommand to the
     * replicated metadata log, and returns the assigned job ID to the client once
     * the command is accepted by a majority.
     */
    grpc::Status SubmitJob(
        grpc::ServerContext*,
        const rpc::SubmitJobRequest* request,
        rpc::SubmitJobReply* reply) override {
        std::string job_id;
        std::string leader_hint;
        std::string leader_id;

        {
            std::lock_guard<std::mutex> lock(impl.mutex);

            auto* node = impl.findPrimaryUnsafe(node_id);
            leader_hint = impl.currentLeaderAddressUnsafe();

            if (node == nullptr || !node->alive || !node->leader) {
                reply->set_leader_hint(leader_hint);
                reply->set_accepted(false);
                return grpc::Status::OK;
            }

            /*
            * Reserve the job id immediately while holding the mutex.
            * Without this, concurrent SubmitJob RPCs can all read the same
            * next_job_id before the replicated SubmitJobCommand is applied,
            * causing multiple clients to receive the same job id and overwrite
            * each other's metadata.
            */
            job_id = "job-" + std::to_string(node->state.next_job_id++);
            leader_id = node_id;
        }

        const bool accepted = impl.replicateSubmitJobUnsafe(leader_id, job_id, *request);

        reply->set_job_id(job_id);
        reply->set_accepted(accepted);
        reply->set_leader_hint(leader_hint);

        return grpc::Status::OK;
    }

        /**
         * Returns the current status and task-completion counters for a job.
         *
         * The query is served from the leader's committed control-plane state.
         */
        grpc::Status GetJobStatus(
            grpc::ServerContext*,
            const rpc::JobStatusRequest* request,
            rpc::JobStatusReply* reply) override {
            std::lock_guard<std::mutex> lock(impl.mutex);
            const auto* node = impl.leaderNodeUnsafe();
            if (node == nullptr) {
                return grpc::Status(grpc::StatusCode::UNAVAILABLE, "no leader");
            }
            reply->set_leader_hint(node->address);
            auto it = node->state.jobs.find(request->job_id());
            if (it == node->state.jobs.end()) {
                return grpc::Status(grpc::StatusCode::NOT_FOUND, "unknown job");
            }
            reply->set_job_id(request->job_id());
            reply->set_state(toProtoJobState(it->second.state));
            reply->set_total_tasks(static_cast<std::uint32_t>(it->second.map_tasks.size() + it->second.reduce_tasks.size()));
            std::size_t completed = 0;
            for (const auto& task : it->second.map_tasks) {
                if (task.state == TaskState::Completed) {
                    ++completed;
                }
            }
            for (const auto& task : it->second.reduce_tasks) {
                if (task.state == TaskState::Completed) {
                    ++completed;
                }
            }
            reply->set_completed_tasks(static_cast<std::uint32_t>(completed));
            return grpc::Status::OK;
        }
        /**
         * Returns final job output if the job has completed.
         *
         * The result is stored in the leader's replicated job metadata after all map
         * and reduce tasks have completed.
         */
        grpc::Status GetJobResult(
            grpc::ServerContext*,
            const rpc::JobResultRequest* request,
            rpc::JobResultReply* reply) override {
            std::lock_guard<std::mutex> lock(impl.mutex);
            const auto* node = impl.leaderNodeUnsafe();
            if (node == nullptr) {
                return grpc::Status(grpc::StatusCode::UNAVAILABLE, "no leader");
            }
            reply->set_leader_hint(node->address);
            auto it = node->state.jobs.find(request->job_id());
            if (it == node->state.jobs.end()) {
                return grpc::Status(grpc::StatusCode::NOT_FOUND, "unknown job");
            }
            reply->set_job_id(request->job_id());
            reply->set_state(toProtoJobState(it->second.state));
            for (const auto& [word, count] : it->second.final_output) {
                auto* wc = reply->add_counts();
                wc->set_word(word);
                wc->set_count(count);
            }
            return grpc::Status::OK;
        }

        /**
         * Registers a worker with the current leader.
         *
         * Registration is replicated through the metadata log so worker membership is
         * preserved across primary failover. If the RPC reaches a follower, the worker
         * receives a leader hint and retries later.
         */
        grpc::Status RegisterWorker(
            grpc::ServerContext*,
            const rpc::RegisterWorkerRequest* request,
            rpc::RegisterWorkerReply* reply) override {
            std::string leader_hint;
            bool is_leader = false;
            {
                std::lock_guard<std::mutex> lock(impl.mutex);
                auto* node = impl.findPrimaryUnsafe(node_id);
                leader_hint = impl.currentLeaderAddressUnsafe();
                is_leader = node != nullptr && node->alive && node->leader;
            }
            if (!is_leader) {
                reply->set_accepted(false);
                reply->set_leader_hint(leader_hint);
                return grpc::Status::OK;
            }

            reply->set_accepted(impl.replicateRegisterWorkerUnsafe(node_id, *request));
            reply->set_leader_hint(leader_hint);
            {
                std::lock_guard<std::mutex> lock(impl.mutex);
                auto* node = impl.findPrimaryUnsafe(node_id);
                if (node != nullptr) {
                    auto worker_it = node->state.workers.find(request->worker_id());
                    if (worker_it != node->state.workers.end()) {
                        worker_it->second.last_heartbeat = Clock::now();
                        worker_it->second.alive = true;
                        worker_it->second.available = true;
                    }
                }
            }
            return grpc::Status::OK;
        }

        /**
         * Receives periodic liveness heartbeats from registered workers.
         *
         * Heartbeats refresh the worker's last-seen timestamp. The scheduler uses this
         * timestamp to detect missed heartbeats and requeue tasks from failed workers.
         */
        grpc::Status Heartbeat(
            grpc::ServerContext*,
            const rpc::HeartbeatRequest* request,
            rpc::HeartbeatReply* reply) override {
            std::lock_guard<std::mutex> lock(impl.mutex);
            auto* node = impl.findPrimaryUnsafe(node_id);
            const auto leader_hint = impl.currentLeaderAddressUnsafe();
            if (node == nullptr || !node->alive || !node->leader) {
                reply->set_accepted(false);
                reply->set_leader_hint(leader_hint);
                return grpc::Status::OK;
            }
            auto worker_it = node->state.workers.find(request->worker_id());
            if (worker_it == node->state.workers.end()) {
                reply->set_accepted(false);
                reply->set_leader_hint(leader_hint);
                return grpc::Status::OK;
            }
            worker_it->second.last_heartbeat = Clock::now();
            worker_it->second.alive = true;
            reply->set_accepted(true);
            reply->set_leader_hint(leader_hint);
            return grpc::Status::OK;
        }

        Impl& impl;
        std::string node_id;
    };

    /**
     * gRPC service for the simplified Raft-style primary replication protocol.
     *
     * The implementation provides enough replicated-log and leader-failover
     * behavior for the lab setting: voting, leader hints, full-log append, commit
     * index propagation, and deterministic state-machine application.
     */
    struct ConsensusServiceImpl final : rpc::ApolloConsensus::Service {
        explicit ConsensusServiceImpl(Impl& owner, std::string id)
            : impl(owner), node_id(std::move(id)) {}

        /**
         * Handles a vote request from a candidate primary.
         *
         * A primary grants its vote if the candidate is in the current term, the node
         * has not already voted for another candidate, and the candidate's log is at
         * least as up to date as the receiver's log.
         */
        grpc::Status RequestVote(
            grpc::ServerContext*,
            const rpc::RequestVoteRequest* request,
            rpc::RequestVoteReply* reply) override {
            std::lock_guard<std::mutex> lock(impl.mutex);
            auto* node = impl.findPrimaryUnsafe(node_id);
            if (node == nullptr || !node->alive) {
                return grpc::Status(grpc::StatusCode::UNAVAILABLE, "primary down");
            }

            if (request->term() > node->current_term) {
                node->current_term = request->term();
                node->leader = false;
                node->voted_for.clear();
                node->leader_hint.clear();
            }

            const auto [last_index, last_term] = impl.lastLogInfoUnsafe(*node);
            const bool up_to_date =
                request->last_log_term() > last_term ||
                (request->last_log_term() == last_term && request->last_log_index() >= last_index);
            const bool can_vote =
                request->term() == node->current_term &&
                (node->voted_for.empty() || node->voted_for == request->candidate_id()) &&
                up_to_date;

            if (can_vote) {
                node->voted_for = request->candidate_id();
                node->last_leader_contact = Clock::now();
            }

            reply->set_term(node->current_term);
            reply->set_vote_granted(can_vote);
            return grpc::Status::OK;
        }

        /**
         * Handles replicated log updates and leader heartbeats.
         *
         * The leader sends the current replicated command log and commit index to each
         * follower. Followers update their local log, advance their commit index, and
         * apply newly committed commands to their control-plane state machine.
         */
        grpc::Status AppendEntries(
            grpc::ServerContext*,
            const rpc::AppendEntriesRequest* request,
            rpc::AppendEntriesReply* reply) override {
            std::lock_guard<std::mutex> lock(impl.mutex);
            auto* node = impl.findPrimaryUnsafe(node_id);
            if (node == nullptr || !node->alive) {
                return grpc::Status(grpc::StatusCode::UNAVAILABLE, "primary down");
            }
            if (request->term() < node->current_term) {
                reply->set_term(node->current_term);
                reply->set_success(false);
                reply->set_match_index(static_cast<std::int64_t>(node->log.size()));
                return grpc::Status::OK;
            }

            node->current_term = request->term();
            node->leader = false;
            node->leader_hint = impl.addressForPrimaryUnsafe(request->leader_id());
            node->last_leader_contact = Clock::now();
            node->voted_for.clear();

            node->log.assign(request->entries().begin(), request->entries().end());
            node->commit_index = std::min<std::int64_t>(request->leader_commit(), static_cast<std::int64_t>(node->log.size()));
            impl.applyCommittedEntriesUnsafe(*node);

            reply->set_term(node->current_term);
            reply->set_success(true);
            reply->set_match_index(static_cast<std::int64_t>(node->log.size()));
            return grpc::Status::OK;
        }

        Impl& impl;
        std::string node_id;
    };

    /**
     * gRPC service exposed by each worker.
     *
     * Workers execute map and reduce tasks assigned by the primary. The service
     * also checks whether the worker is still live so crash tests can interrupt
     * an in-flight task.
     */
    struct WorkerServiceImpl final : rpc::ApolloWorker::Service {
        explicit WorkerServiceImpl(WorkerRuntime& runtime_ref)
            : runtime(runtime_ref) {}

        /**
         * Executes one map or reduce task.
         *
         * Map tasks count words in one input partition. Reduce tasks merge the
         * intermediate counts assigned to one reduce bucket. The method also simulates
         * execution latency using the worker speed multiplier so tests can model
         * stragglers and speculative execution.
         */
        grpc::Status ExecuteTask(
            grpc::ServerContext*,
            const rpc::ExecuteTaskRequest* request,
            rpc::ExecuteTaskReply* reply) override {
            if (!runtime.live_server.load()) {
                return grpc::Status(grpc::StatusCode::UNAVAILABLE, "worker offline");
            }

            auto base_duration = std::chrono::milliseconds(request->phase() == rpc::TASK_PHASE_MAP ? 90 : 120);
            auto total_duration = std::chrono::milliseconds(
                static_cast<int>(base_duration.count() * std::max(0.1, runtime.config.speed_multiplier)));

            auto elapsed = std::chrono::milliseconds::zero();
            while (elapsed < total_duration) {
                if (!runtime.live_server.load()) {
                    return grpc::Status(grpc::StatusCode::UNAVAILABLE, "worker offline");
                }
                constexpr auto step = std::chrono::milliseconds(20);
                std::this_thread::sleep_for(step);
                elapsed += step;
            }

            std::unordered_map<std::string, int> result;
            if (request->phase() == rpc::TASK_PHASE_MAP) {
                result = countWords(request->input_partition());
            } else {
                result = readCounts(request->map_outputs());
            }

            reply->set_success(true);
            fillCounts(result, reply->mutable_counts());
            return grpc::Status::OK;
        }

        WorkerRuntime& runtime;
    };

    /**
     * Starts the Apollo runtime.
     *
     * This creates primary replicas, starts their gRPC servers, and launches the
     * scheduler and consensus background loops. Repeated calls are safe.
     */
    void start() {
        std::lock_guard<std::mutex> lock(mutex);
        if (started) {
            return;
        }
        shutdown.store(false);
        started = true;
        startPrimariesUnsafe();
        scheduler_thread = std::thread([this] { schedulerLoop(); });
        consensus_thread = std::thread([this] { consensusLoop(); });
    }

    /**
     * Stops the Apollo runtime and releases all owned resources.
     *
     * The shutdown path stops background loops, worker servers, heartbeat threads,
     * in-flight task RPC threads, and primary gRPC servers. This prevents tests
     * from passing while detached background work still references destroyed state.
     */
    void stop() {
        {
            std::lock_guard<std::mutex> lock(mutex);
            if (!started) {
                return;
            }
            shutdown.store(true);
            cv.notify_all();
        }

        if (scheduler_thread.joinable()) {
            scheduler_thread.join();
        }
        if (consensus_thread.joinable()) {
            consensus_thread.join();
        }

        for (auto& [_, runtime] : workers) {
            runtime->stop_requested.store(true);
            runtime->live_server.store(false);
            if (runtime->server != nullptr) {
                runtime->server->Shutdown();
            }
        }

        /*
        * Wait for all in-flight task RPC threads before destroying
        * workers, primaries, services, or the Impl mutex. Otherwise a detached task
        * thread can access destroyed state after a test has already passed.
        */
        joinTaskThreads();
        for (auto& [_, runtime] : workers) {
            if (runtime->heartbeat_thread.joinable()) {
                runtime->heartbeat_thread.join();
            }
            if (runtime->server_thread.joinable()) {
                runtime->server_thread.join();
            }
        }

        for (const auto& node_id : primary_order) {
            auto& node = *primaries.at(node_id);
            node.alive = false;
            if (node.server != nullptr) {
                node.server->Shutdown();
            }
        }
        for (const auto& node_id : primary_order) {
            auto& node = *primaries.at(node_id);
            if (node.server_thread.joinable()) {
                node.server_thread.join();
            }
        }

        std::lock_guard<std::mutex> lock(mutex);
        workers.clear();
        primary_services.clear();
        consensus_services.clear();
        primaries.clear();
        primary_order.clear();
        started = false;
    }

    /**
     * Starts the runtime on demand if it has not already been started.
     */
    void ensureStarted() {
        if (!started) {
            start();
        }
    }

    /**
     * Creates and starts all primary replicas.
     *
     * This method must be called while holding the implementation mutex. Each
     * primary receives a localhost gRPC server, and the first primary is selected
     * as the initial leader for deterministic test startup.
     */
    void startPrimariesUnsafe() {
        for (std::size_t i = 0; i < config.primary_replica_count; ++i) {
            auto runtime = std::make_unique<PrimaryRuntime>();
            runtime->node_id = "primary-" + std::to_string(i);
            runtime->address = localhostWildcardAddress();
            runtime->leader = (i == 0);
            runtime->leader_hint = runtime->leader ? runtime->address : "";
            runtime->last_leader_contact = Clock::now();
            primary_order.push_back(runtime->node_id);
            primaries[runtime->node_id] = std::move(runtime);
        }

        for (const auto& node_id : primary_order) {
            auto primary_service = std::make_unique<PrimaryServiceImpl>(*this, node_id);
            auto consensus_service = std::make_unique<ConsensusServiceImpl>(*this, node_id);
            auto& node = *primaries.at(node_id);

            grpc::ServerBuilder builder;
            int selected_port = 0;
            builder.AddListeningPort(node.address, grpc::InsecureServerCredentials(), &selected_port);
            builder.RegisterService(primary_service.get());
            builder.RegisterService(consensus_service.get());
            node.server = builder.BuildAndStart();
            if (node.server == nullptr || selected_port == 0) {
                throw std::runtime_error("Failed to start primary gRPC server");
            }
            node.address = localhostAddress(selected_port);
            node.server_thread = std::thread([server = node.server.get()] {
                server->Wait();
            });

            primary_services[node_id] = std::move(primary_service);
            consensus_services[node_id] = std::move(consensus_service);
        }
    }

    /**
     * Finds a primary runtime by node ID.
     *
     * The Unsafe suffix means the caller must already hold the implementation
     * mutex or otherwise guarantee safe access.
     */
    PrimaryRuntime* findPrimaryUnsafe(const std::string& node_id) {
        auto it = primaries.find(node_id);
        return it == primaries.end() ? nullptr : it->second.get();
    }

    /**
     * Returns the current live leader, if one exists.
     *
     * The Unsafe suffix means the caller must already hold the implementation
     * mutex.
     */
    const PrimaryRuntime* leaderNodeUnsafe() const {
        for (const auto& node_id : primary_order) {
            const auto& node = *primaries.at(node_id);
            if (node.alive && node.leader) {
                return &node;
            }
        }
        return nullptr;
    }

    /**
     * Mutable overload of leaderNodeUnsafe.
     */
    PrimaryRuntime* leaderNodeUnsafe() {
        return const_cast<PrimaryRuntime*>(std::as_const(*this).leaderNodeUnsafe());
    }

    /**
     * Returns the gRPC address of the current leader, or an empty string if no
     * leader is currently available.
     */
    std::string currentLeaderAddressUnsafe() const {
        const auto* leader = leaderNodeUnsafe();
        return leader == nullptr ? "" : leader->address;
    }

    /**
     * Looks up the current gRPC address for a primary replica.
     */
    std::string addressForPrimaryUnsafe(const std::string& node_id) const {
        auto it = primaries.find(node_id);
        return it == primaries.end() ? "" : it->second->address;
    }

    /**
     * Returns the index and term of the last log entry for a primary.
     *
     * Empty logs report index zero and term zero.
     */
    std::pair<std::int64_t, std::int64_t> lastLogInfoUnsafe(const PrimaryRuntime& node) const {
        if (node.log.empty()) {
            return {0, 0};
        }
        return {node.log.back().index(), node.log.back().term()};
    }

    /**
     * Creates a gRPC client stub for the ApolloPrimary service at the given
     * address.
     */
    std::unique_ptr<rpc::ApolloPrimary::Stub> primaryStubForAddress(const std::string& address) const {
        return rpc::ApolloPrimary::NewStub(grpc::CreateChannel(address, grpc::InsecureChannelCredentials()));
    }

    /**
     * Creates a gRPC client stub for the ApolloWorker service at the given
     * address.
     */
    std::unique_ptr<rpc::ApolloWorker::Stub> workerStubForAddress(const std::string& address) const {
        return rpc::ApolloWorker::NewStub(grpc::CreateChannel(address, grpc::InsecureChannelCredentials()));
    }

    /**
     * Starts the gRPC server for a worker runtime.
     *
     * The server binds to an ephemeral localhost port and records the selected
     * address so the primary can later dispatch tasks to this worker.
     */
    void startWorkerServer(WorkerRuntime& runtime) {
        runtime.live_server.store(true);
        runtime.stop_requested.store(false);

        runtime.address = localhostWildcardAddress();
        runtime.service = std::make_unique<WorkerServiceImpl>(runtime);

        grpc::ServerBuilder builder;
        int selected_port = 0;
        builder.AddListeningPort(runtime.address, grpc::InsecureServerCredentials(), &selected_port);
        builder.RegisterService(runtime.service.get());

        runtime.server = builder.BuildAndStart();
        if (runtime.server == nullptr || selected_port == 0) {
            throw std::runtime_error("Failed to start worker gRPC server");
        }

        runtime.address = localhostAddress(selected_port);
        runtime.server_thread = std::thread([server = runtime.server.get()] {
            server->Wait();
        });
    }

    /**
     * Starts the worker heartbeat thread.
     *
     * The heartbeat loop first registers the worker with the current leader and
     * then periodically sends heartbeat RPCs. If the leader changes or rejects the
     * heartbeat, the worker returns to registration mode and tries again.
     */
    void startWorkerHeartbeatThread(WorkerRuntime& runtime) {
        const std::string worker_id = runtime.config.worker_id;

        runtime.heartbeat_thread = std::thread([this, &runtime, worker_id] {
            bool registered = false;

            while (!shutdown.load() && !runtime.stop_requested.load()) {
                if (!runtime.live_server.load()) {
                    break;
                }

                auto leader_address = currentLeaderAddress();
                if (leader_address.empty()) {
                    std::this_thread::sleep_for(config.heartbeat_interval);
                    continue;
                }

                auto stub = primaryStubForAddress(leader_address);

                if (!registered) {
                    grpc::ClientContext context;
                    context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(200));

                    rpc::RegisterWorkerRequest request;
                    request.set_worker_id(worker_id);
                    request.set_worker_address(runtime.address);
                    request.set_speed_multiplier(runtime.config.speed_multiplier);

                    rpc::RegisterWorkerReply reply;
                    auto status = stub->RegisterWorker(&context, request, &reply);
                    registered = status.ok() && reply.accepted();
                } else {
                    grpc::ClientContext context;
                    context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(200));

                    rpc::HeartbeatRequest request;
                    request.set_worker_id(worker_id);

                    rpc::HeartbeatReply reply;
                    auto status = stub->Heartbeat(&context, request, &reply);

                    if (!status.ok() || !reply.accepted()) {
                        registered = false;
                    }
                }

                std::this_thread::sleep_for(config.heartbeat_interval);
            }
        });
    }

    /**
     * Checks whether an active worker assignment still refers to the expected task
     * attempt.
     *
     * This guard prevents stale duplicate completions from releasing a worker or
     * modifying metadata for a newer assignment.
     */
    bool activeTaskMatches(
        const ActiveTask& active,
        const std::string& job_id,
        TaskPhase phase,
        int task_index,
        int attempt_id) const {
        return active.job_id == job_id &&
            active.phase == phase &&
            active.task_index == task_index &&
            active.attempt_id == attempt_id;
    }

    /**
     * Releases a worker only if it is still running the specified task attempt.
     *
     * Speculative execution and retries can create stale completions. This helper
     * keeps worker availability updates idempotent and safe under duplicate task
     * attempts.
     */
    void releaseWorkerIfStillRunningAttemptUnsafe(
        ControlPlaneState& state,
        const std::string& worker_id,
        const std::string& job_id,
        TaskPhase phase,
        int task_index,
        int attempt_id) {
        auto active_it = state.active_tasks_by_worker.find(worker_id);
        if (active_it == state.active_tasks_by_worker.end()) {
            return;
        }

        if (!activeTaskMatches(active_it->second, job_id, phase, task_index, attempt_id)) {
            return;
        }

        state.active_tasks_by_worker.erase(active_it);

        auto worker_it = state.workers.find(worker_id);
        if (worker_it != state.workers.end() && worker_it->second.alive) {
            worker_it->second.available = true;
        }
    }

    /**
     * Requeues in-flight tasks after primary leader failover.
     *
     * A crashed leader may have dispatched tasks whose completion RPCs will never
     * reach the new leader. The new leader therefore conservatively marks running
     * attempts as failed and returns their logical tasks to the queue. Completed
     * tasks remain completed, making this recovery action safe and idempotent.
     */
    void requeueRunningTasksAfterFailoverUnsafe(PrimaryRuntime& leader) {
        auto& state = leader.state;

        for (auto& [_, worker] : state.workers) {
            if (worker.registered && worker.alive) {
                worker.available = true;
                worker.last_heartbeat = Clock::now();
            }
        }

        state.active_tasks_by_worker.clear();

        for (auto& [_, job] : state.jobs) {
            if (job.state != JobState::Running) {
                continue;
            }

            auto requeue_phase = [](std::vector<TaskRecord>& tasks) {
                for (auto& task : tasks) {
                    if (task.state == TaskState::Running || task.state == TaskState::Assigned) {
                        task.state = TaskState::Requeued;
                        for (auto& attempt : task.attempts) {
                            if (attempt.state == TaskState::Running || attempt.state == TaskState::Assigned) {
                                attempt.state = TaskState::Failed;
                            }
                        }
                    }
                }
            };

            requeue_phase(job.map_tasks);
            requeue_phase(job.reduce_tasks);
        }
    }

    /**
     * Replicates a worker-registration command through the primary log.
     */
    std::unique_ptr<rpc::ApolloConsensus::Stub> consensusStubForAddress(const std::string& address) const {
        return rpc::ApolloConsensus::NewStub(grpc::CreateChannel(address, grpc::InsecureChannelCredentials()));
    }

    /**
     * Replicates a worker-registration command through the primary log.
     */
    bool replicateRegisterWorkerUnsafe(const std::string& leader_id, const rpc::RegisterWorkerRequest& request) {
        rpc::LogCommand command;
        auto* payload = command.mutable_register_worker();
        payload->set_worker_id(request.worker_id());
        payload->set_worker_address(request.worker_address());
        payload->set_speed_multiplier(request.speed_multiplier());
        return replicateCommandUnsafe(leader_id, command);
    }

    /**
     * Replicates a job-submission command through the primary log.
     */
    bool replicateSubmitJobUnsafe(const std::string& leader_id, const std::string& job_id, const rpc::SubmitJobRequest& request) {
        rpc::LogCommand command;
        auto* payload = command.mutable_submit_job();
        payload->set_job_id(job_id);
        payload->set_name(request.name());
        payload->set_input(request.input());
        payload->set_map_tasks(request.map_tasks());
        payload->set_reduce_tasks(request.reduce_tasks());
        return replicateCommandUnsafe(leader_id, command);
    }

    /**
     * Replicates a task-assignment command through the primary log.
     *
     * Once committed, the task enters the running state and the selected worker is
     * marked unavailable until the attempt completes or fails.
     */
    bool replicateAssignTaskUnsafe(
        const std::string& leader_id,
        const std::string& job_id,
        TaskPhase phase,
        int task_index,
        int attempt_id,
        const std::string& worker_id,
        bool speculative) {
        rpc::LogCommand command;
        auto* payload = command.mutable_assign_task();
        payload->set_job_id(job_id);
        payload->set_phase(toProtoPhase(phase));
        payload->set_task_index(task_index);
        payload->set_attempt_id(attempt_id);
        payload->set_worker_id(worker_id);
        payload->set_speculative(speculative);
        return replicateCommandUnsafe(leader_id, command);
    }

    /**
     * Replicates a task-completion command through the primary log.
     *
     * The committed command marks exactly one task attempt as authoritative and
     * ignores or fails later duplicate completions for the same logical task.
     */
    bool replicateCompleteTaskUnsafe(
        const std::string& leader_id,
        const std::string& job_id,
        TaskPhase phase,
        int task_index,
        int attempt_id,
        const std::string& worker_id,
        const std::unordered_map<std::string, int>& counts) {
        rpc::LogCommand command;
        auto* payload = command.mutable_complete_task();
        payload->set_job_id(job_id);
        payload->set_phase(toProtoPhase(phase));
        payload->set_task_index(task_index);
        payload->set_attempt_id(attempt_id);
        payload->set_worker_id(worker_id);
        fillCounts(counts, payload->mutable_counts());
        return replicateCommandUnsafe(leader_id, command);
    }

    /**
     * Replicates a worker-failure command through the primary log.
     *
     * Applying this command marks the worker unavailable and requeues any
     * unfinished task attempt assigned to that worker.
     */
    bool replicateWorkerFailedUnsafe(const std::string& leader_id, const std::string& worker_id) {
        rpc::LogCommand command;
        command.mutable_worker_failed()->set_worker_id(worker_id);
        return replicateCommandUnsafe(leader_id, command);
    }

    /**
     * Appends and replicates one metadata command from the current leader.
     *
     * The leader appends the command locally, sends the resulting log to followers,
     * waits for a majority, advances its commit index, and applies committed
     * commands to its local control-plane state.
     *
     * This is a simplified Raft-style path for the lab environment. It is designed
     * to preserve scheduling metadata across tested leader failures rather than to
     * implement every production Raft optimization.
     */
    bool replicateCommandUnsafe(const std::string& leader_id, rpc::LogCommand command) {
        rpc::AppendEntriesRequest request;
        std::int64_t original_term = 0;
        std::vector<std::string> follower_ids;
        {
            std::lock_guard<std::mutex> lock(mutex);
            auto* leader = findPrimaryUnsafe(leader_id);
            if (leader == nullptr || !leader->alive || !leader->leader) {
                return false;
            }

            command.set_term(leader->current_term);
            command.set_index(static_cast<std::int64_t>(leader->log.size()) + 1);
            leader->log.push_back(command);
            original_term = leader->current_term;

            request.set_term(leader->current_term);
            request.set_leader_id(leader->node_id);
            request.set_leader_commit(leader->commit_index);
            for (const auto& entry : leader->log) {
                *request.add_entries() = entry;
            }

            for (const auto& node_id : primary_order) {
                if (node_id != leader_id) {
                    follower_ids.push_back(node_id);
                }
            }
        }

        int successes = 1;
        std::int64_t highest_term_seen = original_term;
        for (const auto& node_id : follower_ids) {
            std::string address;
            bool alive = false;
            {
                std::lock_guard<std::mutex> lock(mutex);
                const auto* follower = findPrimaryUnsafe(node_id);
                alive = follower != nullptr && follower->alive;
                if (alive) {
                    address = follower->address;
                }
            }
            if (!alive) {
                continue;
            }

            auto stub = consensusStubForAddress(address);
            grpc::ClientContext context;
            context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(200));
            rpc::AppendEntriesReply reply;
            auto status = stub->AppendEntries(&context, request, &reply);
            if (status.ok() && reply.success()) {
                ++successes;
            } else if (status.ok()) {
                highest_term_seen = std::max(highest_term_seen, reply.term());
            }
        }

        const int majority = static_cast<int>(primary_order.size() / 2 + 1);
        {
            std::lock_guard<std::mutex> lock(mutex);
            auto* leader = findPrimaryUnsafe(leader_id);
            if (leader == nullptr) {
                return false;
            }
            if (highest_term_seen > leader->current_term) {
                leader->current_term = highest_term_seen;
                leader->leader = false;
                leader->leader_hint.clear();
                return false;
            }
            if (successes < majority) {
                return false;
            }
            leader->commit_index = static_cast<std::int64_t>(leader->log.size());
            applyCommittedEntriesUnsafe(*leader);
            cv.notify_all();
        }

        sendHeartbeatUnsafe(leader_id);
        return true;
    }

    /**
     * Sends a heartbeat AppendEntries RPC from the leader to all live followers.
     *
     * Heartbeats also carry the leader's current log and commit index so followers
     * can apply newly committed metadata commands.
     */
    void sendHeartbeatUnsafe(const std::string& leader_id) {
        rpc::AppendEntriesRequest request;
        std::vector<std::string> follower_addresses;
        {
            std::lock_guard<std::mutex> lock(mutex);
            auto* leader = findPrimaryUnsafe(leader_id);
            if (leader == nullptr || !leader->alive || !leader->leader) {
                return;
            }
            request.set_term(leader->current_term);
            request.set_leader_id(leader->node_id);
            request.set_leader_commit(leader->commit_index);
            for (const auto& entry : leader->log) {
                *request.add_entries() = entry;
            }
            for (const auto& node_id : primary_order) {
                if (node_id == leader_id) {
                    continue;
                }
                const auto& follower = *primaries.at(node_id);
                if (follower.alive) {
                    follower_addresses.push_back(follower.address);
                }
            }
        }

        for (const auto& address : follower_addresses) {
            auto stub = consensusStubForAddress(address);
            grpc::ClientContext context;
            context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(150));
            rpc::AppendEntriesReply reply;
            (void)stub->AppendEntries(&context, request, &reply);
        }
    }

    /**
     * Applies all committed but unapplied log entries to a primary's state machine.
     */
    void applyCommittedEntriesUnsafe(PrimaryRuntime& node) {
        while (node.last_applied < node.commit_index) {
            ++node.last_applied;
            const auto& entry = node.log.at(static_cast<std::size_t>(node.last_applied - 1));
            applyCommandUnsafe(node, entry);
        }
    }

    /**
     * Applies one replicated metadata command to a primary's control-plane state.
     *
     * This function is the state-machine transition point for worker registration,
     * job creation, task assignment, task completion, and worker failure. All
     * primary replicas reach the same metadata state by applying the same committed
     * commands in log order.
     */
    void applyCommandUnsafe(PrimaryRuntime& node, const rpc::LogCommand& entry) {
        auto& state = node.state;
        switch (entry.payload_case()) {
            case rpc::LogCommand::kRegisterWorker: {
                const auto& cmd = entry.register_worker();
                auto& worker = state.workers[cmd.worker_id()];
                worker.worker_id = cmd.worker_id();
                worker.address = cmd.worker_address();
                worker.registered = true;
                worker.alive = true;
                worker.available = true;
                worker.speed_multiplier = cmd.speed_multiplier();
                worker.last_heartbeat = Clock::now();
                break;
            }
            case rpc::LogCommand::kSubmitJob: {
                const auto& cmd = entry.submit_job();
                JobRecord job;
                job.job_id = cmd.job_id();
                job.name = cmd.name().empty() ? cmd.job_id() : cmd.name();
                job.state = JobState::Running;
                job.reducers = std::max(1, cmd.reduce_tasks());
                job.input_partitions = partitionInput(cmd.input(), cmd.map_tasks());
                for (std::size_t i = 0; i < job.input_partitions.size(); ++i) {
                    TaskRecord task;
                    task.task_index = static_cast<int>(i);
                    task.phase = TaskPhase::Map;
                    job.map_tasks.push_back(task);
                }
                state.jobs[job.job_id] = std::move(job);
                state.next_job_id = std::max(state.next_job_id, std::stoi(cmd.job_id().substr(4)) + 1);
                break;
            }
            case rpc::LogCommand::kAssignTask: {
                const auto& cmd = entry.assign_task();
                auto& job = state.jobs.at(cmd.job_id());
                auto& tasks = cmd.phase() == rpc::TASK_PHASE_REDUCE ? job.reduce_tasks : job.map_tasks;
                auto& task = tasks.at(static_cast<std::size_t>(cmd.task_index()));
                auto& worker = state.workers.at(cmd.worker_id());
                task.state = TaskState::Running;
                task.attempts.push_back(AttemptRecord{
                    cmd.attempt_id(),
                    cmd.worker_id(),
                    cmd.speculative(),
                    TaskState::Running,
                    Clock::now(),
                });
                if (cmd.speculative()) {
                    task.speculative_launched = true;
                }
                worker.available = false;
                worker.alive = true;
                state.active_tasks_by_worker[cmd.worker_id()] = ActiveTask{
                    cmd.job_id(),
                    fromProtoPhase(cmd.phase()),
                    cmd.task_index(),
                    cmd.attempt_id(),
                };
                break;
            }
            case rpc::LogCommand::kCompleteTask: {
                const auto& cmd = entry.complete_task();
                auto& job = state.jobs.at(cmd.job_id());
                const TaskPhase phase = fromProtoPhase(cmd.phase());
                auto& tasks = phase == TaskPhase::Reduce ? job.reduce_tasks : job.map_tasks;
                auto& task = tasks.at(static_cast<std::size_t>(cmd.task_index()));

                auto attempt_it = std::find_if(
                    task.attempts.begin(),
                    task.attempts.end(),
                    [&](const AttemptRecord& attempt) {
                        return attempt.attempt_id == cmd.attempt_id();
                    });

                if (attempt_it == task.attempts.end()) {
                    break;
                }

                if (task.state == TaskState::Completed) {
                    attempt_it->state = TaskState::Failed;
                    releaseWorkerIfStillRunningAttemptUnsafe(
                        state,
                        cmd.worker_id(),
                        cmd.job_id(),
                        phase,
                        cmd.task_index(),
                        cmd.attempt_id());
                    break;
                }

                /*
                * If the attempt was already failed/requeued, ignore its late completion.
                * This can happen after worker failure or leader failover.
                */
                if (attempt_it->state != TaskState::Running &&
                    attempt_it->state != TaskState::Assigned) {
                    releaseWorkerIfStillRunningAttemptUnsafe(
                        state,
                        cmd.worker_id(),
                        cmd.job_id(),
                        phase,
                        cmd.task_index(),
                        cmd.attempt_id());
                    break;
                }

                attempt_it->state = TaskState::Completed;
                task.state = TaskState::Completed;
                task.authoritative_attempt_id = cmd.attempt_id();
                task.output = readCounts(cmd.counts());

                releaseWorkerIfStillRunningAttemptUnsafe(
                    state,
                    cmd.worker_id(),
                    cmd.job_id(),
                    phase,
                    cmd.task_index(),
                    cmd.attempt_id());

                /*
                * Mark all other running duplicates as failed. Again, only release their
                * workers if the active record still matches that old duplicate attempt.
                */
                for (auto& other_attempt : task.attempts) {
                    if (other_attempt.attempt_id == cmd.attempt_id()) {
                        continue;
                    }

                    if (other_attempt.state == TaskState::Running ||
                        other_attempt.state == TaskState::Assigned) {
                        other_attempt.state = TaskState::Failed;

                        releaseWorkerIfStillRunningAttemptUnsafe(
                            state,
                            other_attempt.worker_id,
                            cmd.job_id(),
                            phase,
                            cmd.task_index(),
                            other_attempt.attempt_id);
                    }
                }

                updateJobPhasesUnsafe(node, job);
                break;
            }
            case rpc::LogCommand::kWorkerFailed: {
                const auto& cmd = entry.worker_failed();
                auto worker_it = state.workers.find(cmd.worker_id());
                if (worker_it == state.workers.end()) {
                    break;
                }
                worker_it->second.alive = false;
                worker_it->second.available = false;
                auto active_it = state.active_tasks_by_worker.find(cmd.worker_id());
                if (active_it != state.active_tasks_by_worker.end()) {
                    auto& active = active_it->second;
                    auto& job = state.jobs.at(active.job_id);
                    auto& tasks = active.phase == TaskPhase::Reduce ? job.reduce_tasks : job.map_tasks;
                    auto& task = tasks.at(static_cast<std::size_t>(active.task_index));
                    if (task.state != TaskState::Completed) {
                        for (auto& attempt : task.attempts) {
                            if (attempt.attempt_id == active.attempt_id && attempt.state == TaskState::Running) {
                                attempt.state = TaskState::Failed;
                            }
                        }
                        task.state = TaskState::Requeued;
                    }
                    state.active_tasks_by_worker.erase(active_it);
                }
                break;
            }
            case rpc::LogCommand::PAYLOAD_NOT_SET:
            default:
                break;
        }
    }

    /**
     * Advances a job from map phase to reduce phase, or from reduce phase to done.
     *
     * Once all map tasks complete, reduce tasks are created. Once all reduce tasks
     * complete, their outputs are merged into the final deterministic job result.
     */
    void updateJobPhasesUnsafe(PrimaryRuntime& node, JobRecord& job) {
        const bool maps_done = !job.map_tasks.empty() &&
                               std::all_of(job.map_tasks.begin(), job.map_tasks.end(), [](const TaskRecord& task) {
                                   return task.state == TaskState::Completed;
                               });

        if (maps_done && job.reduce_tasks.empty()) {
            for (int i = 0; i < job.reducers; ++i) {
                TaskRecord task;
                task.task_index = i;
                task.phase = TaskPhase::Reduce;
                job.reduce_tasks.push_back(task);
            }
        }

        const bool reduces_done = !job.reduce_tasks.empty() &&
                                  std::all_of(job.reduce_tasks.begin(), job.reduce_tasks.end(), [](const TaskRecord& task) {
                                      return task.state == TaskState::Completed;
                                  });
        if (maps_done && reduces_done) {
            std::map<std::string, int> final_counts;
            for (const auto& reduce_task : job.reduce_tasks) {
                for (const auto& [word, count] : reduce_task.output) {
                    final_counts[word] += count;
                }
            }
            job.final_output = std::move(final_counts);
            job.state = JobState::Completed;
        }
        (void)node;
    }

    /**
     * Background scheduler loop.
     *
     * On each iteration, the leader detects timed-out workers, plans speculative
     * attempts, assigns queued tasks, replicates each scheduling decision, and then
     * dispatches committed task attempts to workers over gRPC.
     */
    void schedulerLoop() {
        while (true) {
            std::vector<std::string> failed_workers;
            std::vector<PlannedAssignment> assignments;
            std::string leader_id;
            {
                std::lock_guard<std::mutex> lock(mutex);
                if (shutdown.load()) {
                    return;
                }
                auto* leader = leaderNodeUnsafe();
                if (leader != nullptr) {
                    leader_id = leader->node_id;
                    failed_workers = collectTimedOutWorkersUnsafe(*leader);
                    maybeLaunchSpeculativeAttemptsUnsafe(*leader, assignments);
                    assignQueuedTasksUnsafe(*leader, assignments);
                }
            }

            for (const auto& worker_id : failed_workers) {
                replicateWorkerFailedUnsafe(leader_id, worker_id);
            }
            for (const auto& assignment : assignments) {
                if (replicateAssignTaskUnsafe(
                        assignment.leader_id,
                        assignment.job_id,
                        assignment.phase,
                        assignment.task_index,
                        assignment.attempt_id,
                        assignment.worker_id,
                        assignment.speculative)) {
                    dispatchTask(
                        assignment.leader_id,
                        assignment.worker_id,
                        assignment.phase,
                        assignment.task_index,
                        assignment.attempt_id,
                        assignment.speculative);
                }
            }

            std::this_thread::sleep_for(config.scheduler_interval);
        }
    }

    /**
     * Background scheduler loop.
     *
     * On each iteration, the leader detects timed-out workers, plans speculative
     * attempts, assigns queued tasks, replicates each scheduling decision, and then
     * dispatches committed task attempts to workers over gRPC.
     */
    void consensusLoop() {
        const auto election_timeout = std::max<std::chrono::milliseconds>(config.worker_timeout, std::chrono::milliseconds(250));
        while (true) {
            std::string leader_id;
            {
                std::lock_guard<std::mutex> lock(mutex);
                if (shutdown.load()) {
                    return;
                }
                auto* leader = leaderNodeUnsafe();
                if (leader != nullptr) {
                    leader_id = leader->node_id;
                } else {
                    const auto now = Clock::now();
                    for (const auto& node_id : primary_order) {
                        auto& node = *primaries.at(node_id);
                        if (!node.alive) {
                            continue;
                        }
                        if (now - node.last_leader_contact >= election_timeout) {
                            leader_id = node.node_id;
                            break;
                        }
                    }
                }
            }
            if (!leader_id.empty()) {
                bool currently_leader = false;
                {
                    std::lock_guard<std::mutex> lock(mutex);
                    auto* node = findPrimaryUnsafe(leader_id);
                    currently_leader = node != nullptr && node->alive && node->leader;
                }
                if (currently_leader) {
                    sendHeartbeatUnsafe(leader_id);
                } else {
                    startElectionUnsafe(leader_id);
                }
            }
            std::this_thread::sleep_for(config.heartbeat_interval);
        }
    }

    /**
     * Starts a simplified leader election for a candidate primary.
     *
     * The candidate increments its term, requests votes from live peers, and
     * becomes leader after receiving a majority. After election, in-flight tasks
     * are conservatively requeued so scheduling can continue safely.
     */
    void startElectionUnsafe(const std::string& candidate_id) {
        std::int64_t term = 0;
        std::int64_t last_log_index = 0;
        std::int64_t last_log_term = 0;
        std::vector<std::string> peer_ids;
        {
            std::lock_guard<std::mutex> lock(mutex);
            auto* candidate = findPrimaryUnsafe(candidate_id);
            if (candidate == nullptr || !candidate->alive) {
                return;
            }
            candidate->current_term += 1;
            candidate->voted_for = candidate_id;
            candidate->leader = false;
            term = candidate->current_term;
            std::tie(last_log_index, last_log_term) = lastLogInfoUnsafe(*candidate);
            for (const auto& node_id : primary_order) {
                if (node_id != candidate_id) {
                    peer_ids.push_back(node_id);
                }
            }
        }

        int votes = 1;
        std::int64_t highest_term_seen = term;
        for (const auto& node_id : peer_ids) {
            std::string address;
            bool alive = false;
            {
                std::lock_guard<std::mutex> lock(mutex);
                auto* peer = findPrimaryUnsafe(node_id);
                alive = peer != nullptr && peer->alive;
                if (alive) {
                    address = peer->address;
                }
            }
            if (!alive) {
                continue;
            }
            auto stub = consensusStubForAddress(address);
            grpc::ClientContext context;
            context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(200));
            rpc::RequestVoteRequest request;
            request.set_term(term);
            request.set_candidate_id(candidate_id);
            request.set_last_log_index(last_log_index);
            request.set_last_log_term(last_log_term);
            rpc::RequestVoteReply reply;
            auto status = stub->RequestVote(&context, request, &reply);
            if (status.ok() && reply.vote_granted()) {
                ++votes;
            } else if (status.ok()) {
                highest_term_seen = std::max(highest_term_seen, reply.term());
            }
        }

        std::lock_guard<std::mutex> lock(mutex);
        auto* candidate = findPrimaryUnsafe(candidate_id);
        if (candidate == nullptr || !candidate->alive) {
            return;
        }
        if (highest_term_seen > candidate->current_term) {
            candidate->current_term = highest_term_seen;
            candidate->leader = false;
            return;
        }
        if (votes >= static_cast<int>(primary_order.size() / 2 + 1)) {
            for (const auto& node_id : primary_order) {
                auto& node = *primaries.at(node_id);
                node.leader = (node_id == candidate_id);
                if (node.leader) {
                    node.leader_hint = node.address;
                } else {
                    node.leader_hint = candidate->address;
                }
            }

            candidate->last_leader_contact = Clock::now();

            /*
            * The old leader may have dispatched tasks whose completions will be sent
            * to the old leader id and therefore lost. After failover, conservatively
            * requeue all in-flight tasks on the new leader. Completed tasks remain
            * completed, so this is safe and idempotent.
            */
            requeueRunningTasksAfterFailoverUnsafe(*candidate);
        }
    }

    /**
     * Finds workers whose heartbeats have timed out.
     *
     * The returned worker IDs are later converted into replicated WorkerFailed
     * commands outside the scheduler's critical section.
     */
    std::vector<std::string> collectTimedOutWorkersUnsafe(PrimaryRuntime& leader) {
        const auto now = Clock::now();
        std::vector<std::string> failed_workers;
        for (const auto& [worker_id, worker] : leader.state.workers) {
            if (worker.registered && worker.alive && now - worker.last_heartbeat > config.worker_timeout) {
                failed_workers.push_back(worker_id);
            }
        }
        return failed_workers;
    }

    /**
     * Selects an available healthy worker for a task.
     *
     * The scheduler avoids workers already running another attempt of the same
     * logical task and avoids workers already reserved by other planned assignments
     * in the current scheduling cycle.
     */
    std::optional<std::string> selectAvailableWorkerUnsafe(
        const PrimaryRuntime& leader,
        const TaskRecord& task,
        const std::set<std::string>& reserved_workers) const {
        std::set<std::string> in_use;
        for (const auto& attempt : task.attempts) {
            if (attempt.state == TaskState::Running) {
                in_use.insert(attempt.worker_id);
            }
        }
        for (const auto& [worker_id, worker] : leader.state.workers) {
            if (worker.registered && worker.alive && worker.available &&
                in_use.find(worker_id) == in_use.end() &&
                reserved_workers.find(worker_id) == reserved_workers.end()) {
                return worker_id;
            }
        }
        return std::nullopt;
    }

    /**
     * Computes the next globally unique task-attempt ID.
     *
     * Attempt IDs are unique across all jobs and phases in the current replicated
     * leader state.
     */
    int nextAttemptIdUnsafe(const PrimaryRuntime& leader) const {
        int max_attempt = 0;
        for (const auto& [_, job] : leader.state.jobs) {
            for (const auto& task : job.map_tasks) {
                for (const auto& attempt : task.attempts) {
                    max_attempt = std::max(max_attempt, attempt.attempt_id);
                }
            }
            for (const auto& task : job.reduce_tasks) {
                for (const auto& attempt : task.attempts) {
                    max_attempt = std::max(max_attempt, attempt.attempt_id);
                }
            }
        }
        return max_attempt + 1;
    }

    /**
     * Plans speculative duplicate attempts for slow running tasks.
     *
     * A task becomes eligible when it has been running longer than the configured
     * speculative threshold, has not already launched a speculative copy, and a
     * different healthy worker is available.
     */
    void maybeLaunchSpeculativeAttemptsUnsafe(
        PrimaryRuntime& leader,
        std::vector<PlannedAssignment>& assignments) {
        const auto now = Clock::now();
        std::set<std::string> reserved_workers;
        for (const auto& assignment : assignments) {
            reserved_workers.insert(assignment.worker_id);
        }
        int next_attempt = nextAttemptIdUnsafe(leader);
        for (auto& entry : leader.state.jobs) {
            auto& job_id = entry.first;
            auto& job = entry.second;
            if (job.state != JobState::Running) {
                continue;
            }

            auto maybe_phase = [&](std::vector<TaskRecord>& tasks, TaskPhase phase) {
                for (auto& task : tasks) {
                    if (task.state != TaskState::Running || task.speculative_launched || task.attempts.empty()) {
                        continue;
                    }
                    const auto& latest_attempt = task.attempts.back();
                    if (now - latest_attempt.started_at <= config.speculative_threshold) {
                        continue;
                    }
                    auto worker_id = selectAvailableWorkerUnsafe(leader, task, reserved_workers);
                    if (!worker_id.has_value()) {
                        continue;
                    }
                    assignments.push_back(PlannedAssignment{
                        leader.node_id,
                        job_id,
                        *worker_id,
                        phase,
                        task.task_index,
                        next_attempt++,
                        true,
                    });
                    reserved_workers.insert(*worker_id);
                }
            };

            maybe_phase(job.map_tasks, TaskPhase::Map);
            if (!job.reduce_tasks.empty()) {
                maybe_phase(job.reduce_tasks, TaskPhase::Reduce);
            }
        }
    }

    /**
     * Plans assignments for queued or requeued tasks.
     *
     * Map tasks are assigned first. Reduce tasks are assigned only after all map
     * tasks for the job have completed and reduce tasks have been created.
     */
    void assignQueuedTasksUnsafe(
        PrimaryRuntime& leader,
        std::vector<PlannedAssignment>& assignments) {
        std::set<std::string> reserved_workers;
        for (const auto& assignment : assignments) {
            reserved_workers.insert(assignment.worker_id);
        }
        int next_attempt = nextAttemptIdUnsafe(leader);
        for (auto& entry : leader.state.jobs) {
            auto& job_id = entry.first;
            auto& job = entry.second;
            if (job.state != JobState::Running) {
                continue;
            }

            auto assign_phase = [&](std::vector<TaskRecord>& tasks, TaskPhase phase) {
                for (auto& task : tasks) {
                    if (task.state != TaskState::Queued && task.state != TaskState::Requeued) {
                        continue;
                    }
                    auto worker_id = selectAvailableWorkerUnsafe(leader, task, reserved_workers);
                    if (!worker_id.has_value()) {
                        return;
                    }
                    assignments.push_back(PlannedAssignment{
                        leader.node_id,
                        job_id,
                        *worker_id,
                        phase,
                        task.task_index,
                        next_attempt++,
                        false,
                    });
                    reserved_workers.insert(*worker_id);
                }
            };

            const bool maps_done = std::all_of(job.map_tasks.begin(), job.map_tasks.end(), [](const TaskRecord& task) {
                return task.state == TaskState::Completed;
            });
            assign_phase(job.map_tasks, TaskPhase::Map);
            if (maps_done && !job.reduce_tasks.empty()) {
                assign_phase(job.reduce_tasks, TaskPhase::Reduce);
            }
        }
    }

    /**
     * Joins all in-flight task RPC threads.
     *
     * Task dispatch uses one thread per attempt. During shutdown, all such threads
     * must finish before workers, primaries, or shared metadata are destroyed.
     */
    void joinTaskThreads() {
        std::vector<std::thread> local_threads;

        {
            std::lock_guard<std::mutex> lock(mutex);
            local_threads.swap(task_threads);
        }

        for (auto& thread : local_threads) {
            if (thread.joinable()) {
                thread.join();
            }
        }
    }

    /**
     * Dispatches a committed task attempt to a worker over gRPC.
     *
     * The method builds the ExecuteTask request from committed job metadata, calls
     * the worker's ExecuteTask RPC, and replicates the completion only if the RPC
     * succeeds. Late completions are handled safely by the replicated completion
     * logic.
     */
    void dispatchTask(
        const std::string& leader_id,
        const std::string& worker_id,
        TaskPhase phase,
        int task_index,
        int attempt_id,
        bool speculative) {
        (void)speculative;

        std::thread task_thread([this, leader_id, worker_id, phase, task_index, attempt_id] {
            rpc::ExecuteTaskRequest request;
            std::string job_id;
            std::string worker_address;

            {
                std::lock_guard<std::mutex> lock(mutex);

                if (shutdown.load()) {
                    return;
                }

                auto* leader = findPrimaryUnsafe(leader_id);
                if (leader == nullptr || !leader->alive || !leader->leader) {
                    return;
                }

                const auto active_it = leader->state.active_tasks_by_worker.find(worker_id);
                if (active_it == leader->state.active_tasks_by_worker.end()) {
                    return;
                }

                const auto& active = active_it->second;
                if (!activeTaskMatches(active, active.job_id, phase, task_index, attempt_id)) {
                    return;
                }

                job_id = active.job_id;
                const auto& job = leader->state.jobs.at(job_id);

                request.set_job_id(job_id);
                request.set_phase(toProtoPhase(phase));
                request.set_task_index(task_index);
                request.set_attempt_id(attempt_id);
                request.set_reducers(job.reducers);

                if (phase == TaskPhase::Map) {
                    request.set_input_partition(job.input_partitions.at(static_cast<std::size_t>(task_index)));
                } else {
                    std::unordered_map<std::string, int> bucket;

                    for (const auto& map_task : job.map_tasks) {
                        for (const auto& [word, count] : map_task.output) {
                            if (static_cast<int>(
                                    std::hash<std::string>{}(word) %
                                    static_cast<std::size_t>(job.reducers)) == task_index) {
                                bucket[word] += count;
                            }
                        }
                    }

                    fillCounts(bucket, request.mutable_map_outputs());
                }

                worker_address = leader->state.workers.at(worker_id).address;
            }

            auto stub = workerStubForAddress(worker_address);

            grpc::ClientContext context;
            context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(3));

            rpc::ExecuteTaskReply reply;
            auto status = stub->ExecuteTask(&context, request, &reply);

            if (!status.ok() || !reply.success()) {
                return;
            }

            if (shutdown.load()) {
                return;
            }

            replicateCompleteTaskUnsafe(
                leader_id,
                job_id,
                phase,
                task_index,
                attempt_id,
                worker_id,
                readCounts(reply.counts()));
        });

        {
            std::lock_guard<std::mutex> lock(mutex);
            task_threads.push_back(std::move(task_thread));
        }
    }

    /**
     * Adds a worker runtime to the in-process cluster harness.
     *
     * The worker starts a real localhost gRPC server and a heartbeat thread. It
     * registers asynchronously with the current primary leader.
     */
    std::string addWorker(const WorkerConfig& worker_config) {
        ensureStarted();

        const auto worker_id = worker_config.worker_id.empty()
            ? "worker-" + std::to_string(workers.size())
            : worker_config.worker_id;

        auto runtime = std::make_unique<WorkerRuntime>(
            WorkerConfig{worker_id, worker_config.speed_multiplier},
            localhostWildcardAddress());

        startWorkerServer(*runtime);
        startWorkerHeartbeatThread(*runtime);

        {
            std::lock_guard<std::mutex> lock(mutex);
            workers[worker_id] = std::move(runtime);
        }

        return worker_id;
    }

    /**
     * Returns the current leader address using a mutex-protected lookup.
     */
    std::string currentLeaderAddress() const {
        std::lock_guard<std::mutex> lock(mutex);
        return currentLeaderAddressUnsafe();
    }

    /**
     * Simulates a worker crash and records the failure through replicated metadata.
     *
     * The worker runtime is stopped locally, and a WorkerFailed command is appended
     * to the control-plane log so unfinished work can be requeued consistently.
     */
    void crashWorker(const std::string& worker_id) {
        WorkerRuntime* runtime = nullptr;

        {
            std::lock_guard<std::mutex> lock(mutex);
            auto it = workers.find(worker_id);
            if (it == workers.end()) {
                return;
            }

            runtime = it->second.get();

            /*
            * Stop the worker process/runtime, but do not directly mark the worker
            * as dead in replicated primary state here. Worker failure must go
            * through the replicated WorkerFailedCommand path so that any in-flight
            * task is failed/requeued consistently on the primary replicas.
            */
            runtime->live_server.store(false);
            runtime->stop_requested.store(true);
        }

        if (runtime->server != nullptr) {
            runtime->server->Shutdown();
        }

        if (runtime->heartbeat_thread.joinable()) {
            runtime->heartbeat_thread.join();
        }

        if (runtime->server_thread.joinable()) {
            runtime->server_thread.join();
        }

        /*
        * Inject the failure into the replicated metadata log immediately.
        * This makes tests deterministic: the task assigned to this worker is
        * requeued right away instead of waiting for heartbeat timeout.
        *
        * If replication fails temporarily, the normal heartbeat timeout path can
        * still detect the missing worker later because we did not pre-mark the
        * worker as dead in leader state above.
        */
        std::string leader_id;
        {
            std::lock_guard<std::mutex> lock(mutex);
            auto* leader = leaderNodeUnsafe();
            if (leader != nullptr) {
                leader_id = leader->node_id;

                auto worker_it = leader->state.workers.find(worker_id);
                if (worker_it != leader->state.workers.end()) {
                    worker_it->second.last_heartbeat =
                        Clock::now() - config.worker_timeout - std::chrono::milliseconds(50);
                }
            }
        }

        if (!leader_id.empty()) {
            (void)replicateWorkerFailedUnsafe(leader_id, worker_id);
        }
    }

    /**
     * Restarts a previously crashed worker runtime.
     *
     * The worker starts a fresh gRPC server and heartbeat loop, then re-registers
     * with the current leader.
     */
    void restartWorker(const std::string& worker_id) {
        WorkerRuntime* runtime = nullptr;

        {
            std::lock_guard<std::mutex> lock(mutex);
            auto it = workers.find(worker_id);
            if (it == workers.end()) {
                return;
            }
            runtime = it->second.get();
        }

        if (runtime->server != nullptr) {
            runtime->server->Shutdown();
        }

        if (runtime->heartbeat_thread.joinable()) {
            runtime->heartbeat_thread.join();
        }

        if (runtime->server_thread.joinable()) {
            runtime->server_thread.join();
        }

        runtime->server.reset();
        runtime->service.reset();

        startWorkerServer(*runtime);
        startWorkerHeartbeatThread(*runtime);
    }

    /**
     * Updates the simulated speed multiplier for a worker.
     *
     * Larger values make task execution slower and are useful for testing
     * speculative execution.
     */
    void setWorkerSpeed(const std::string& worker_id, double speed_multiplier) {
        std::lock_guard<std::mutex> lock(mutex);
        auto it = workers.find(worker_id);
        if (it != workers.end()) {
            it->second->config.speed_multiplier = speed_multiplier;
        }
        auto* leader = leaderNodeUnsafe();
        if (leader != nullptr) {
            auto worker_it = leader->state.workers.find(worker_id);
            if (worker_it != leader->state.workers.end()) {
                worker_it->second.speed_multiplier = speed_multiplier;
            }
        }
    }

    /**
     * Submits a word-count job through the current leader.
     *
     * The method retries while leader election is in progress and returns the
     * cluster-assigned job ID after the leader accepts the submission.
    */
    std::string submitWordCountJob(const JobRequest& request) {
        ensureStarted();
        for (int attempt = 0; attempt < 40; ++attempt) {
            const auto leader_address = currentLeaderAddress();
            if (!leader_address.empty()) {
                auto stub = primaryStubForAddress(leader_address);
                grpc::ClientContext context;
                context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(1));
                rpc::SubmitJobRequest rpc_request;
                rpc_request.set_name(request.name);
                rpc_request.set_input(request.input);
                rpc_request.set_map_tasks(request.map_tasks);
                rpc_request.set_reduce_tasks(request.reduce_tasks);
                rpc::SubmitJobReply reply;
                auto status = stub->SubmitJob(&context, rpc_request, &reply);
                if (status.ok() && reply.accepted()) {
                    return reply.job_id();
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        throw std::runtime_error("Unable to submit job through leader");
    }

    /**
     * Queries the current leader for a job summary.
     */
    JobSummary getJobSummary(const std::string& job_id) const {
        const auto leader_address = currentLeaderAddress();
        if (leader_address.empty()) {
            throw std::runtime_error("No leader available");
        }
        auto stub = primaryStubForAddress(leader_address);
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(1));
        rpc::JobStatusRequest request;
        request.set_job_id(job_id);
        rpc::JobStatusReply reply;
        auto status = stub->GetJobStatus(&context, request, &reply);
        if (!status.ok()) {
            throw std::runtime_error("Unable to query job status");
        }
        return JobSummary{
            reply.job_id(),
            fromProtoJobState(reply.state()),
            reply.total_tasks(),
            reply.completed_tasks(),
        };
    }

    /**
     * Polls the current leader until a job completes or the timeout expires.
     *
     * When the job completes, the final word-count result is returned as a sorted
     * map.
     */
    std::map<std::string, int> waitForJobResult(const std::string& job_id, std::chrono::milliseconds timeout) {
        const auto deadline = Clock::now() + timeout;
        while (Clock::now() < deadline) {
            const auto leader_address = currentLeaderAddress();
            if (!leader_address.empty()) {
                auto stub = primaryStubForAddress(leader_address);
                grpc::ClientContext context;
                context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(500));
                rpc::JobResultRequest request;
                request.set_job_id(job_id);
                rpc::JobResultReply reply;
                auto status = stub->GetJobResult(&context, request, &reply);
                if (status.ok() && fromProtoJobState(reply.state()) == JobState::Completed) {
                    std::map<std::string, int> result;
                    for (const auto& wc : reply.counts()) {
                        result[wc.word()] += wc.count();
                    }
                    return result;
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        throw std::runtime_error("Timed out waiting for job");
    }

    /**
     * Simulates failure of the current primary leader.
     *
     * A surviving primary can later win an election and continue scheduling from
     * replicated metadata.
     */
    void crashLeader() {
        std::unique_ptr<grpc::Server> server;
        {
            std::lock_guard<std::mutex> lock(mutex);
            auto* leader = leaderNodeUnsafe();
            if (leader == nullptr) {
                return;
            }
            leader->alive = false;
            leader->leader = false;
            leader->last_leader_contact = Clock::now() - config.worker_timeout - std::chrono::milliseconds(50);
            server = std::move(leader->server);
        }
        if (server != nullptr) {
            server->Shutdown();
        }
    }

    /**
     * Returns the node ID of the current primary leader.
     */
    std::string currentLeaderId() const {
        std::lock_guard<std::mutex> lock(mutex);
        const auto* leader = leaderNodeUnsafe();
        return leader == nullptr ? "" : leader->node_id;
    }

    /**
     * Returns all worker IDs known to the local test harness.
     */
    std::vector<std::string> workerIds() const {
        std::lock_guard<std::mutex> lock(mutex);
        std::vector<std::string> ids;
        ids.reserve(workers.size());
        for (const auto& [worker_id, _] : workers) {
            ids.push_back(worker_id);
        }
        std::sort(ids.begin(), ids.end());
        return ids;
    }
};

/*
 * Public ApolloCluster API forwarding methods.
 *
 * The detailed API documentation lives in include/apollo/cluster.h. These
 * definitions simply delegate to the private implementation object.
 */
/**
 * Constructs an ApolloCluster facade backed by a private implementation.
 */
ApolloCluster::ApolloCluster(ClusterConfig config)
    : impl_(std::make_unique<Impl>(std::move(config))) {}

/**
 * Destroys the cluster facade and stops the runtime through Impl cleanup.
 */
ApolloCluster::~ApolloCluster() = default;

void ApolloCluster::start() {
    impl_->start();
}

void ApolloCluster::stop() {
    impl_->stop();
}

std::string ApolloCluster::addWorker(const WorkerConfig& config) {
    return impl_->addWorker(config);
}

void ApolloCluster::crashWorker(const std::string& worker_id) {
    impl_->crashWorker(worker_id);
}

void ApolloCluster::restartWorker(const std::string& worker_id) {
    impl_->restartWorker(worker_id);
}

void ApolloCluster::setWorkerSpeed(const std::string& worker_id, double speed_multiplier) {
    impl_->setWorkerSpeed(worker_id, speed_multiplier);
}

std::string ApolloCluster::submitWordCountJob(const JobRequest& request) {
    return impl_->submitWordCountJob(request);
}

JobSummary ApolloCluster::getJobSummary(const std::string& job_id) const {
    return impl_->getJobSummary(job_id);
}

std::map<std::string, int> ApolloCluster::waitForJobResult(
    const std::string& job_id,
    std::chrono::milliseconds timeout) {
    return impl_->waitForJobResult(job_id, timeout);
}

void ApolloCluster::crashLeader() {
    impl_->crashLeader();
}

std::string ApolloCluster::currentLeaderId() const {
    return impl_->currentLeaderId();
}

std::vector<std::string> ApolloCluster::workerIds() const {
    return impl_->workerIds();
}

/**
 * Converts a final word-count map into deterministic line-oriented text.
 *
 * Each output line contains one word followed by its count. The input map is
 * ordered, so output is stable across runs.
 */
std::string formatWordCount(const std::map<std::string, int>& counts) {
    std::ostringstream output;
    for (const auto& [word, count] : counts) {
        output << word << ' ' << count << '\n';
    }
    return output.str();
}

}  // namespace apollo
