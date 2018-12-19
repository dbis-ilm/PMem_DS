cmake_minimum_required(VERSION 3.3)
project (nvm-DS)

set(CMAKE_MACOSX_RPATH 1)

################################################################################
# customization section                                                        #
################################################################################

# Set the mount path of your pmem device where the structures should be stored
set(PMEM_MNT_PATH "/mnt/pmem/test")

option(ENABLE_LOG         "enables log output for e.g. debugging"        OFF)
option(ENABLE_PROFILING   "enables profiling for some of the structures" ON )
option(BUILD_TEST_CASES   "build tests for functionality of structures"  ON )
option(BUILD_GOOGLE_BENCH "build google benchmark"                       ON )
option(BUILD_BENCHMARKS   "build benchmarks for structures"              ON )

################################################################################
# End of customization section                                                 #
################################################################################

if(ENABLE_LOG)
  add_definitions("-DENABLE_LOG")
  set(LOG 1)
else()
  set(LOG 0)
endif()

if(ENABLE_PROFILING)
  add_definitions("-DENABLE_PROFILING")
  set(PROFILING 1)
else()
  set(PROFILING 0)
endif()

# Benchmark test requires benchmark library
if (BUILD_BENCHMARKS)
  set(BUILD_GOOGLE_BENCH ON)
endif()

# C++ compiler flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -Wall -g -O0 -Wsign-compare")
if ("${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unused-local-typedefs -Wno-#pragma-messages")
elseif("${CMAKE_CXX_COMPILER_ID}" MATCHES "GNU")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unused-local-typedefs -U_FORTIFY_SOURCE -D_FORTIFY_SOURCE=0 -Wno-unused")
elseif("${CMAKE_CXX_COMPILER_ID}" MATCHES "Intel")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -wd488 -wd597")
endif()

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wno-unused -Wno-uninitialized")

# show used compiler
message("Using Compiler: ${CMAKE_CXX_COMPILER_ID}.")

# Add our CMake directory to CMake's module path
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/../cmake/")

# We download some 3rdparty modules from github.com before building the project.
include(Download3rdParty)

if(NOT ${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -pthread")
set(DYLIB_LIBRARY "-ldl")
else()
set(DYLIB_LIBRARY "")
endif()

############################
# Google benchmark library #
############################
#
if (BUILD_GOOGLE_BENCH)
  add_custom_target(BENCHMARK_DEPS ALL DEPENDS ${THIRD_PARTY_DIR}/benchmark)
  set(TEST_INCLUDE_DIRS ${THIRD_PARTY_DIR}/benchmark/include
    CACHE INTERNAL "TESTING: Include Directories" FORCE)
  set (BENCHMARK_LIB "${THIRD_PARTY_DIR}/benchmark/lib/libbenchmark.a")
else()
  set (BENCHMARK_LIB "")
endif()

#####################################
# Persistent Memory Development Kit #
#####################################
#
set (PMDK_LIBRARIES
  ${THIRD_PARTY_DIR}/pmdk/lib/libpmemblk.a
  ${THIRD_PARTY_DIR}/pmdk/lib/libpmemlog.a
  ${THIRD_PARTY_DIR}/pmdk/lib/libpmemobj.a
  ${THIRD_PARTY_DIR}/pmdk/lib/libpmempool.a
  ${THIRD_PARTY_DIR}/pmdk/lib/libpmem.a
  ${DYLIB_LIBRARY}
)

#################################
# Pre-built common dependencies #
#################################
#
set(dep_sources
  ${THIRD_PARTY_DIR}/fmt
  ${THIRD_PARTY_DIR}/catch
  ${THIRD_PARTY_DIR}/pmdk
  ${THIRD_PARTY_DIR}/pmdk-cpp
)

configure_file(config.h.in ${CMAKE_BINARY_DIR}/generated/config.h)

include_directories(${PROJECT_SOURCE_DIR}
                    ${CMAKE_BINARY_DIR}/generated
                    ${THIRD_PARTY_DIR}
                    ${THIRD_PARTY_DIR}/pmdk/include)
add_library(dependencies SHARED ${dep_sources}) # triggers building of dependencies
target_link_libraries(dependencies ${PMDK_LIBRARIES} ndctl daxctl)
set_target_properties(dependencies PROPERTIES LINKER_LANGUAGE CXX)

#############################
# Including Data Structures #
#############################
#
add_subdirectory(pbptree)   # Persistent version of B⁺-Tree
add_subdirectory(fptree)    # Remake of the FPTree
add_subdirectory(ptable)    # BDCC-based analytical table structure

#########################
# Unit test using Catch #
#########################
#
add_library(Catch INTERFACE)
target_include_directories(Catch INTERFACE test)
enable_testing()
add_subdirectory(test)

#############################################
# Micor-benchmarking using Google Benchmark #
#############################################
#
add_subdirectory(bench)
