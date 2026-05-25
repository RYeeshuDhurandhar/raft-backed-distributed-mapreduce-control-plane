cmake_minimum_required(VERSION 3.16)
project(apollo LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(APPLE AND EXISTS "/opt/homebrew")
    list(APPEND CMAKE_PREFIX_PATH "/opt/homebrew")
    file(GLOB _grpc_prefixes "/opt/homebrew/Cellar/grpc/*")
    file(GLOB _protobuf_prefixes "/opt/homebrew/Cellar/protobuf/*")
    list(APPEND CMAKE_PREFIX_PATH ${_grpc_prefixes} ${_protobuf_prefixes})
endif()

include(FetchContent)

# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------

# Ubuntu/Debian packages usually expose Protobuf through CMake module mode,
# not CONFIG mode. Therefore, do not require CONFIG here.
find_package(Protobuf REQUIRED)

# gRPC packages usually expose CMake config targets through gRPCConfig.cmake.
find_package(gRPC CONFIG REQUIRED)

find_package(Threads REQUIRED)

find_package(GTest CONFIG QUIET)
if(NOT GTest_FOUND)
    find_package(GTest QUIET)
endif()

if(NOT GTest_FOUND)
    FetchContent_Declare(
        googletest
        URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip
        DOWNLOAD_EXTRACT_TIMESTAMP TRUE
    )
    FetchContent_MakeAvailable(googletest)
endif()

find_program(PROTOC_EXECUTABLE protoc REQUIRED)
find_program(GRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin REQUIRED)

# ---------------------------------------------------------------------------
# Protobuf / gRPC code generation
# ---------------------------------------------------------------------------

set(PROTO_DIR ${CMAKE_CURRENT_SOURCE_DIR}/proto)
set(GENERATED_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated)
file(MAKE_DIRECTORY ${GENERATED_DIR})

set(APOLLO_PROTO ${PROTO_DIR}/apollo.proto)

set(APOLLO_PROTO_SRCS
    ${GENERATED_DIR}/apollo.pb.cc
    ${GENERATED_DIR}/apollo.grpc.pb.cc
)

set(APOLLO_PROTO_HDRS
    ${GENERATED_DIR}/apollo.pb.h
    ${GENERATED_DIR}/apollo.grpc.pb.h
)

add_custom_command(
    OUTPUT ${APOLLO_PROTO_SRCS} ${APOLLO_PROTO_HDRS}
    COMMAND ${PROTOC_EXECUTABLE}
    ARGS --proto_path=${PROTO_DIR}
         --cpp_out=${GENERATED_DIR}
         --grpc_out=${GENERATED_DIR}
         --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN_EXECUTABLE}
         ${APOLLO_PROTO}
    DEPENDS ${APOLLO_PROTO}
    VERBATIM
)

# ---------------------------------------------------------------------------
# Generated proto library
# ---------------------------------------------------------------------------

add_library(apollo_proto ${APOLLO_PROTO_SRCS})

target_include_directories(apollo_proto
    PUBLIC
        ${GENERATED_DIR}
        ${Protobuf_INCLUDE_DIRS}
)

target_link_libraries(apollo_proto
    PUBLIC
        ${Protobuf_LIBRARIES}
        gRPC::grpc++
        gRPC::grpc
)

# ---------------------------------------------------------------------------
# Apollo library
# ---------------------------------------------------------------------------

add_library(apollo_lib
    src/cluster.cpp
)

target_include_directories(apollo_lib
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${GENERATED_DIR}
        ${Protobuf_INCLUDE_DIRS}
)

target_link_libraries(apollo_lib
    PUBLIC
        Threads::Threads
        apollo_proto
)

# ---------------------------------------------------------------------------
# Demo CLI
# ---------------------------------------------------------------------------

add_executable(apollo_cli
    src/main.cpp
)

target_link_libraries(apollo_cli
    PRIVATE
        apollo_lib
)

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

enable_testing()

add_executable(apollo_tests
    tests/apollo_tests.cpp
)

target_link_libraries(apollo_tests
    PRIVATE
        apollo_lib
        GTest::gtest
)

# Register the whole GoogleTest binary as one CTest test.
# This is intentional because apollo_tests has a custom GoogleTest listener
# that prints per-test marks and one total marks summary at the end.
add_test(NAME apollo_tests COMMAND apollo_tests --gtest_color=no)
