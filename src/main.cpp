#include "apollo/cluster.h"

#include <fstream>
#include <iostream>
#include <iterator>
#include <stdexcept>

namespace {

/**
 * Reads an entire input file into a string.
 *
 * The demo CLI uses this helper to load the word-count input dataset before
 * submitting it to the Apollo cluster. The file contents are passed directly
 * to the built-in word-count MapReduce job.
 *
 * @param path Path to the input text file.
 * @return Complete file contents.
 * @throws std::runtime_error if the file cannot be opened.
 */
std::string readFile(const std::string& path) {
    std::ifstream input(path);
    if (!input) {
        throw std::runtime_error("Unable to open input file: " + path);
    }
    return std::string(std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>());
}

/**
 * Prints the supported command-line usage.
 *
 * The current CLI exposes one demonstration command:
 *
 *     apollo_cli demo-wordcount <input-file>
 *
 * This command starts a local Apollo cluster, submits a word-count job, waits
 * for completion, and prints the final sorted word-count output.
 */
void printUsage() {
    std::cerr << "Usage: apollo_cli demo-wordcount <input-file>\n";
}

}  // namespace

/**
 * Entry point for the Apollo demonstration CLI.
 *
 * The CLI currently supports a single demo mode, demo-wordcount. In this mode,
 * the program:
 *
 * 1. starts a local Apollo cluster,
 * 2. adds three workers,
 * 3. reads the input file,
 * 4. submits a word-count MapReduce job,
 * 5. waits for the job to complete,
 * 6. and prints the final deterministic word-count result.
 *
 * @param argc Number of command-line arguments.
 * @param argv Command-line argument vector.
 * @return Zero on success, nonzero on usage or runtime error.
 */
int main(int argc, char** argv) {
    if (argc != 3 || std::string(argv[1]) != "demo-wordcount") {
        printUsage();
        return 1;
    }

    try {
        apollo::ApolloCluster cluster;
        cluster.start();
        cluster.addWorker({"worker-a", 1.0});
        cluster.addWorker({"worker-b", 1.0});
        cluster.addWorker({"worker-c", 1.0});

        const auto input = readFile(argv[2]);
        const auto job_id = cluster.submitWordCountJob({
            "demo-wordcount",
            input,
            4,
            2,
        });

        const auto result = cluster.waitForJobResult(job_id, std::chrono::seconds(5));
        std::cout << "job_id=" << job_id << '\n';
        std::cout << apollo::formatWordCount(result);
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << ex.what() << '\n';
        return 1;
    }
}
