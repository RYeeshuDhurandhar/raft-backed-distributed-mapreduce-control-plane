SHELL := /bin/bash

.PHONY: all deps configure build test clean

all: deps configure build test

deps:
	@echo "Checking/installing Apollo dependencies..."
	@if [[ "$$(uname)" == "Darwin" ]]; then \
		if ! command -v brew >/dev/null 2>&1; then \
			echo "Homebrew is not installed. Please install Homebrew first:"; \
			echo '/bin/bash -c "$$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"'; \
			exit 1; \
		fi; \
		brew list cmake >/dev/null 2>&1 || brew install cmake >/dev/null; \
		brew list protobuf >/dev/null 2>&1 || brew install protobuf >/dev/null; \
		brew list grpc >/dev/null 2>&1 || brew install grpc >/dev/null; \
		brew list googletest >/dev/null 2>&1 || brew install googletest >/dev/null; \
	elif command -v apt-get >/dev/null 2>&1; then \
		export DEBIAN_FRONTEND=noninteractive; \
		update_ok=0; \
		for i in {1..30}; do \
			if sudo apt-get -qq update >/dev/null 2>&1; then \
				update_ok=1; \
				break; \
			fi; \
			echo "apt update lock busy or update failed; retrying..."; \
			sleep 5; \
		done; \
		if [[ "$$update_ok" -ne 1 ]]; then \
			echo "apt-get update failed after retries"; \
			exit 1; \
		fi; \
		install_ok=0; \
		for i in {1..30}; do \
			if sudo apt-get -qq install -y \
				build-essential \
				cmake \
				pkg-config \
				protobuf-compiler \
				libprotobuf-dev \
				libgrpc++-dev \
				protobuf-compiler-grpc \
				libgtest-dev >/dev/null 2>&1; then \
				install_ok=1; \
				break; \
			fi; \
			echo "apt install lock busy or install failed; retrying..."; \
			sleep 5; \
		done; \
		if [[ "$$install_ok" -ne 1 ]]; then \
			echo "apt-get install failed after retries"; \
			exit 1; \
		fi; \
	else \
		echo "Unsupported OS/package manager."; \
		echo "Please install: cmake, protobuf, grpc, grpc_cpp_plugin, and googletest."; \
		exit 1; \
	fi

configure:
	@echo "Configuring Apollo..."
	@cmake -S . -B build >/dev/null

build:
	@echo "Building Apollo..."
	@cmake --build build >/dev/null

test:
	@echo "Running Apollo tests..."
	@./build/apollo_tests --gtest_color=no

clean:
	rm -rf build
