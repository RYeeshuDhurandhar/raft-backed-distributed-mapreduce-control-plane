#include "apollo/cluster.h"

#include <gtest/gtest.h>

#include <cctype>
#include <chrono>
#include <future>
#include <iostream>
#include <map>
#include <string>
#include <thread>
#include <unordered_map>

namespace {

/**
 * Computes the expected word-count output directly inside the test process.
 *
 * This helper intentionally mirrors Apollo's built-in word-count tokenization:
 * alphanumeric characters form words, words are lowercased, and non-alphanumeric
 * characters act as separators. The tests compare cluster output against this
 * local reference implementation.
 *
 * @param input Raw input text.
 * @return Deterministic word-count map ordered lexicographically by word.
 */
std::map<std::string, int> expectedWordCount(const std::string& input) {
    std::map<std::string, int> counts;
    std::string word;
    for (char ch : input) {
        if (std::isalnum(static_cast<unsigned char>(ch))) {
            word.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(ch))));
        } else if (!word.empty()) {
            ++counts[word];
            word.clear();
        }
    }
    if (!word.empty()) {
        ++counts[word];
    }
    return counts;
}

/**
 * Builds a larger input string by repeating a phrase.
 *
 * The distributed-systems tests use larger inputs to keep jobs running long
 * enough for fault injection, leader failover, speculative execution, and
 * dynamic worker joins.
 *
 * @param phrase Phrase to repeat.
 * @param times Number of repetitions.
 * @return Generated input string.
 */
std::string repeatedInput(const std::string& phrase, int times) {
    std::string output;
    for (int i = 0; i < times; ++i) {
        output += phrase;
        output.push_back(' ');
    }
    return output;
}

/**
 * Returns the point value assigned to each test.
 *
 * These point values follow the proposal's test allocation:
 *
 * - End-to-end word count: 30 points
 * - Worker crash and reassignment: 35 points
 * - Leader failure and recovery: 45 points
 * - Speculative execution: 35 points
 * - Dynamic worker join: 20 points
 * - Multi-client concurrent job isolation: 35 points
 *
 * The worker restart test is an additional robustness test and is assigned
 * 0 points so that the total remains 200 proposal points.
 */
std::unordered_map<std::string, int> testPointValues() {
    return {
        {"ApolloClusterTest.EndToEndWordCountMatchesExpectedOutput", 25},
        {"ApolloClusterTest.WorkerCrashTriggersTaskReassignment", 30},
        {"ApolloClusterTest.LeaderFailurePreservesReplicatedSchedulingState", 40},
        {"ApolloClusterTest.CrashedWorkerCanRestartAndRejoinCluster", 15},
        {"ApolloClusterTest.SpeculativeExecutionIgnoresLateDuplicateResults", 30},
        {"ApolloClusterTest.DynamicWorkerJoinHelpsLargeJobFinishCorrectly", 20},
        {"ApolloClusterTest.ConcurrentJobsRemainIsolated", 25},
        {"ApolloClusterTest.JobSummaryReportsProgressAndCompletion", 10},
        {"ApolloClusterTest.EmptyAndPunctuationOnlyInputProducesEmptyOutput", 5},
    };
}

/**
 * Computes the total number of proposal points represented by the tests.
 */
int totalAvailablePoints() {
    int total = 0;
    for (const auto& [_, points] : testPointValues()) {
        total += points;
    }
    return total;
}

/**
 * GoogleTest event listener that prints proposal-style marks.
 *
 * After each test finishes, this listener prints either the full point value
 * for the test if it passed, or 0 if it failed. At the end of the test run, it
 * prints the total marks received out of the total available proposal points.
 */
class MarksPrintingListener final : public testing::EmptyTestEventListener {
public:
    void OnTestEnd(const testing::TestInfo& test_info) override {
        const std::string full_name =
            std::string(test_info.test_suite_name()) + "." + test_info.name();

        const auto points_map = testPointValues();
        const auto it = points_map.find(full_name);
        const int possible_points = it == points_map.end() ? 0 : it->second;
        const int earned_points = test_info.result()->Passed() ? possible_points : 0;

        total_earned_ += earned_points;

        std::cout << "\n[MARKS] "
                  << full_name
                  << ": "
                  << earned_points
                  << " / "
                  << possible_points
                  << '\n';
    }

    void OnTestProgramEnd(const testing::UnitTest&) override {
        std::cout << "\n========================================\n";
        std::cout << "Apollo Test Marks Summary\n";
        std::cout << "Total marks received: "
                  << total_earned_
                  << " / "
                  << totalAvailablePoints()
                  << '\n';
        std::cout << "========================================\n";
    }

private:
    int total_earned_{0};
};

/**
 * Validates basic end-to-end MapReduce correctness.
 *
 * This test starts one Apollo cluster, registers three workers, submits a
 * word-count job, waits for completion, and checks that the final output
 * exactly matches the local reference implementation.
 *
 * Covered requirements:
 * - functional MapReduce correctness,
 * - map/reduce task scheduling,
 * - final result collection.
 */
TEST(ApolloClusterTest, EndToEndWordCountMatchesExpectedOutput) {
    apollo::ApolloCluster cluster;
    cluster.start();
    cluster.addWorker({"worker-1", 1.0});
    cluster.addWorker({"worker-2", 1.0});
    cluster.addWorker({"worker-3", 1.0});

    const std::string input = "cat dog cat bird dog cat";
    const auto job_id = cluster.submitWordCountJob({"wordcount", input, 3, 2});
    const auto result = cluster.waitForJobResult(job_id, std::chrono::seconds(5));

    EXPECT_EQ(result, expectedWordCount(input));
}

/**
 * Validates fault-tolerant task reassignment after worker crash.
 *
 * The test submits a long-running job and then crashes one worker while tasks
 * are in flight. The primary should mark the worker failed, requeue any
 * unfinished task assigned to it, assign the task to another healthy worker,
 * and still complete the job with the correct output.
 *
 * Covered requirements:
 * - worker liveness tracking,
 * - worker failure handling,
 * - task requeue and reassignment,
 * - no permanent job stall after one worker crash.
 */
TEST(ApolloClusterTest, WorkerCrashTriggersTaskReassignment) {
    apollo::ApolloCluster cluster;
    cluster.start();
    cluster.addWorker({"worker-1", 1.0});
    cluster.addWorker({"worker-2", 1.0});
    cluster.addWorker({"worker-3", 1.0});

    const std::string input = repeatedInput("alpha beta gamma delta", 120);
    const auto job_id = cluster.submitWordCountJob({"reassign", input, 6, 3});

    std::this_thread::sleep_for(std::chrono::milliseconds(60));
    cluster.crashWorker("worker-1");

    const auto result = cluster.waitForJobResult(job_id, std::chrono::seconds(8));
    EXPECT_EQ(result, expectedWordCount(input));
}

/**
 * Validates primary leader failover with replicated scheduling state.
 *
 * The test submits a job, crashes the current leader while work is in flight,
 * waits for a different primary to become leader, and then verifies that the
 * job still completes correctly. This checks that committed scheduling metadata
 * survives failover and that in-flight work can be recovered safely.
 *
 * Covered requirements:
 * - replicated primary metadata,
 * - leader failure recovery,
 * - deterministic task-state recovery,
 * - continued scheduling after failover.
 */
TEST(ApolloClusterTest, LeaderFailurePreservesReplicatedSchedulingState) {
    apollo::ApolloCluster cluster;
    cluster.start();
    cluster.addWorker({"worker-1", 1.0});
    cluster.addWorker({"worker-2", 1.0});
    cluster.addWorker({"worker-3", 1.0});

    const std::string input = repeatedInput("raft metadata survives failover", 100);
    const auto original_leader = cluster.currentLeaderId();
    const auto job_id = cluster.submitWordCountJob({"leader-failover", input, 5, 2});

    std::this_thread::sleep_for(std::chrono::milliseconds(80));
    cluster.crashLeader();

    std::string new_leader;
    for (int i = 0; i < 50; ++i) {
        new_leader = cluster.currentLeaderId();
        if (!new_leader.empty() && new_leader != original_leader) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    EXPECT_FALSE(new_leader.empty());
    EXPECT_NE(new_leader, original_leader);

    const auto result = cluster.waitForJobResult(job_id, std::chrono::seconds(8));
    EXPECT_EQ(result, expectedWordCount(input));
}

/**
 * Validates that a crashed worker can restart and rejoin the cluster.
 *
 * The test crashes a worker before submitting the job, restarts it, gives it
 * time to register again, and then verifies that the cluster can use the
 * recovered worker while completing a normal word-count job.
 *
 * Covered requirements:
 * - dynamic worker registration,
 * - worker liveness recovery,
 * - cluster operation after worker restart.
 *
 * This is an additional robustness test and is assigned 0 proposal points.
 */
TEST(ApolloClusterTest, CrashedWorkerCanRestartAndRejoinCluster) {
    apollo::ApolloCluster cluster;
    cluster.start();

    cluster.addWorker({"worker-1", 1.0});
    cluster.addWorker({"worker-2", 1.0});

    cluster.crashWorker("worker-1");
    std::this_thread::sleep_for(std::chrono::milliseconds(250));

    cluster.restartWorker("worker-1");
    std::this_thread::sleep_for(std::chrono::milliseconds(250));

    const std::string input = repeatedInput("worker restart rejoins cluster", 80);
    const auto job_id = cluster.submitWordCountJob({"restart-worker", input, 4, 2});
    const auto result = cluster.waitForJobResult(job_id, std::chrono::seconds(8));

    EXPECT_EQ(result, expectedWordCount(input));
}

/**
 * Validates speculative execution and safe duplicate-result handling.
 *
 * The test adds one intentionally slow worker and two normal workers. A slow
 * task should become eligible for speculative execution, causing the primary
 * to launch a duplicate attempt elsewhere. The cluster must accept only one
 * successful completion for the logical task and ignore any late duplicate
 * result.
 *
 * Covered requirements:
 * - straggler mitigation,
 * - speculative duplicate execution,
 * - idempotent task completion,
 * - correct final output despite duplicate attempts.
 */
TEST(ApolloClusterTest, SpeculativeExecutionIgnoresLateDuplicateResults) {
    apollo::ApolloCluster cluster({
        std::chrono::milliseconds(40),
        std::chrono::milliseconds(220),
        std::chrono::milliseconds(20),
        std::chrono::milliseconds(150),
        3,
    });
    cluster.start();
    cluster.addWorker({"worker-slow", 5.0});
    cluster.addWorker({"worker-fast-1", 1.0});
    cluster.addWorker({"worker-fast-2", 1.0});

    const std::string input = repeatedInput("slow worker speculative duplicate", 160);
    const auto job_id = cluster.submitWordCountJob({"speculative", input, 3, 2});

    const auto result = cluster.waitForJobResult(job_id, std::chrono::seconds(8));
    EXPECT_EQ(result, expectedWordCount(input));
}

/**
 * Validates dynamic worker joins while a job is already running.
 *
 * The test starts a job with only one worker, then adds more workers after the
 * job has begun. Newly joined workers should register with the leader and
 * become eligible for future map or reduce task assignments without restarting
 * the cluster.
 *
 * Covered requirements:
 * - dynamic scaling,
 * - worker registration during active jobs,
 * - correct completion after membership changes.
 */
TEST(ApolloClusterTest, DynamicWorkerJoinHelpsLargeJobFinishCorrectly) {
    apollo::ApolloCluster cluster;
    cluster.start();
    cluster.addWorker({"worker-1", 1.0});

    const std::string input = repeatedInput("scale out while running", 240);
    const auto job_id = cluster.submitWordCountJob({"dynamic-join", input, 8, 3});

    std::this_thread::sleep_for(std::chrono::milliseconds(120));
    cluster.addWorker({"worker-2", 1.0});
    cluster.addWorker({"worker-3", 1.0});
    cluster.addWorker({"worker-4", 1.0});

    const auto result = cluster.waitForJobResult(job_id, std::chrono::seconds(8));
    EXPECT_EQ(result, expectedWordCount(input));
}

/**
 * Validates isolation between concurrently submitted jobs.
 *
 * The test submits three independent jobs from separate async clients. The
 * cluster must assign distinct job IDs, maintain separate task metadata and
 * intermediate outputs, and return the correct result for each job without
 * cross-job contamination.
 *
 * Covered requirements:
 * - concurrent job submission,
 * - unique job identifiers,
 * - isolated per-job metadata,
 * - isolated final outputs.
 */
TEST(ApolloClusterTest, ConcurrentJobsRemainIsolated) {
    apollo::ApolloCluster cluster;
    cluster.start();
    cluster.addWorker({"worker-1", 1.0});
    cluster.addWorker({"worker-2", 1.0});
    cluster.addWorker({"worker-3", 1.0});
    cluster.addWorker({"worker-4", 1.0});

    const std::string input_a = repeatedInput("apple banana apple", 90);
    const std::string input_b = repeatedInput("carrot date carrot eggplant", 90);
    const std::string input_c = repeatedInput("fig grape fig honeydew fig", 90);

    auto submit_a = std::async(std::launch::async, [&] {
        return cluster.submitWordCountJob({"job-a", input_a, 4, 2});
    });

    auto submit_b = std::async(std::launch::async, [&] {
        return cluster.submitWordCountJob({"job-b", input_b, 4, 2});
    });

    auto submit_c = std::async(std::launch::async, [&] {
        return cluster.submitWordCountJob({"job-c", input_c, 4, 2});
    });

    const auto job_a = submit_a.get();
    const auto job_b = submit_b.get();
    const auto job_c = submit_c.get();
    EXPECT_NE(job_a, job_b);
    EXPECT_NE(job_a, job_c);
    EXPECT_NE(job_b, job_c);

    EXPECT_EQ(cluster.waitForJobResult(job_a, std::chrono::seconds(8)), expectedWordCount(input_a));
    EXPECT_EQ(cluster.waitForJobResult(job_b, std::chrono::seconds(8)), expectedWordCount(input_b));
    EXPECT_EQ(cluster.waitForJobResult(job_c, std::chrono::seconds(8)), expectedWordCount(input_c));
}

}  // namespace

/**
 * Validates job status reporting while a job is running and after completion.
 *
 * The test submits a job, checks that the job appears in the control plane,
 * waits for completion, and then verifies that the final summary reports the
 * completed state with all known tasks completed.
 *
 * Covered requirements:
 * - job status query support,
 * - task completion accounting,
 * - control-plane metadata visibility.
 */
TEST(ApolloClusterTest, JobSummaryReportsProgressAndCompletion) {
    apollo::ApolloCluster cluster;
    cluster.start();
    cluster.addWorker({"worker-1", 1.0});
    cluster.addWorker({"worker-2", 1.0});
    cluster.addWorker({"worker-3", 1.0});

    const std::string input = repeatedInput("status query validates metadata", 80);
    const auto job_id = cluster.submitWordCountJob({"status-query", input, 4, 2});

    const auto initial_summary = cluster.getJobSummary(job_id);
    EXPECT_EQ(initial_summary.job_id, job_id);
    EXPECT_TRUE(
        initial_summary.state == apollo::JobState::Running ||
        initial_summary.state == apollo::JobState::Completed
    );
    EXPECT_GE(initial_summary.total_tasks, 4U);

    const auto result = cluster.waitForJobResult(job_id, std::chrono::seconds(8));
    EXPECT_EQ(result, expectedWordCount(input));

    const auto final_summary = cluster.getJobSummary(job_id);
    EXPECT_EQ(final_summary.job_id, job_id);
    EXPECT_EQ(final_summary.state, apollo::JobState::Completed);
    EXPECT_EQ(final_summary.completed_tasks, final_summary.total_tasks);
}

/**
 * Validates word-count behavior on empty and punctuation-only input.
 *
 * The expected result is an empty word-count map because there are no
 * alphanumeric tokens.
 *
 * Covered requirements:
 * - edge-case MapReduce correctness,
 * - deterministic tokenization,
 * - safe handling of empty map outputs.
 */
TEST(ApolloClusterTest, EmptyAndPunctuationOnlyInputProducesEmptyOutput) {
    apollo::ApolloCluster cluster;
    cluster.start();
    cluster.addWorker({"worker-1", 1.0});
    cluster.addWorker({"worker-2", 1.0});

    const std::string input = "!!! ,,, ... ---";
    const auto job_id = cluster.submitWordCountJob({"empty-input", input, 3, 2});
    const auto result = cluster.waitForJobResult(job_id, std::chrono::seconds(5));

    EXPECT_TRUE(result.empty());
}

/**
 * Custom GoogleTest entry point.
 *
 * The custom main installs MarksPrintingListener so that each test prints its
 * assigned point value when it passes or 0 when it fails, followed by a total
 * marks summary.
 */
int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);

    testing::TestEventListeners& listeners =
        testing::UnitTest::GetInstance()->listeners();

    listeners.Append(new MarksPrintingListener());

    return RUN_ALL_TESTS();
}
