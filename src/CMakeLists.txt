cmake_minimum_required(VERSION 3.8)
project (nvm-DS)

################################################################################
# customization section                                                        #
################################################################################

# Set the mount path of your pmem device where the structures should be stored
set(PMEM_MNT_PATH "/mnt/pmem/test")

# Installation path (creates include and lib within this directory)
if(NOT PROJECT_INSTALL_DIR)
  set(PROJECT_INSTALL_DIR ${CMAKE_INSTALL_PREFIX})
endif()

option(ENABLE_LOG         "enables log output for e.g. debugging"        OFF)
option(ENABLE_PROFILING   "enables profiling for some of the structures" OFF)
option(BUILD_TEST_CASES   "build tests for functionality of structures"  ON )
option(BUILD_GOOGLE_BENCH "build google benchmark"                       OFF)
option(BUILD_BENCHMARKS   "build benchmarks for structures"              ON )

################################################################################
# End of customization section                                                 #
################################################################################

if(ENABLE_LOG)
  add_definitions("-DENABLE_LOG")
endif()

if(ENABLE_PROFILING)
  add_definitions("-DENABLE_PROFILING")
endif()

# Benchmark test requires benchmark library
if (BUILD_BENCHMARKS)
  set(BUILD_GOOGLE_BENCH ON)
endif()

# C++ compiler flags
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS_DEBUG -g)
set(CMAKE_CXX_FLAGS_RELEASE -O3)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

# show used compiler
message("Using Compiler: ${CMAKE_CXX_COMPILER_ID}.")

set(CMAKE_MACOSX_RPATH 1)

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
  set(TEST_INCLUDE_DIRS ${THIRD_PARTY_DIR}/benchmark/include
    CACHE INTERNAL "TESTING: Include Directories" FORCE)
  set (BENCHMARK_LIB "${THIRD_PARTY_DIR}/benchmark/lib/libbenchmark.a")
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
configure_file(config.h.in ${CMAKE_BINARY_DIR}/generated/config.h)

include_directories(${PROJECT_SOURCE_DIR}
                    ${CMAKE_BINARY_DIR}/generated
                    ${THIRD_PARTY_DIR}
                    ${THIRD_PARTY_DIR}/pmdk/include)
add_library(nvmDS_deps STATIC ${THIRD_PARTY_DIR}/pmdk)
add_dependencies(nvmDS_deps fmt_target catch_target pmdk_target) #< executes custom commands
target_link_libraries(nvmDS_deps ${PMDK_LIBRARIES} stdc++fs ndctl daxctl)
set_target_properties(nvmDS_deps PROPERTIES LINKER_LANGUAGE CXX)

#################
# Install Paths #
#################
#
#set(PROJECT_LIBS ${PROJECT_LIBS})
set(PROJECT_INCLUDES_D ${PROJECT_INCLUDES_D}
                       ${CMAKE_CURRENT_SOURCE_DIR}/utils)
set(PROJECT_INCLUDES_F ${PROJECT_INCLUDES_F}
                       ${CMAKE_BINARY_DIR}/generated/config.h)

set(PROJECT_LIB_DIR ${PROJECT_INSTALL_DIR}/lib)
set(PROJECT_INCLUDE_DIR ${PROJECT_INSTALL_DIR}/include)

#############################
# Including Data Structures #
#############################
#
add_subdirectory(pbptree) # Persistent version of B⁺-Tree
add_subdirectory(fptree)  # Remake of the FPTree
add_subdirectory(wbtree)  # Remake of the wB+Tree
add_subdirectory(ptable)  # BDCC-based analytical table structure

#########################
# Unit test using Catch #
#########################
#
add_library(Catch INTERFACE)
target_include_directories(Catch INTERFACE test)
enable_testing()
add_subdirectory(test)

#############################################
# Micro-benchmarking using Google Benchmark #
#############################################
#
add_subdirectory(bench)

###################
# Install Process #
###################
#
#foreach(LIB ${PROJECT_LIBS})
#  install(TARGETS ${LIB} DESTINATION ${PROJECT_LIB_DIR})
#endforeach(LIB)

install(DIRECTORY ${PROJECT_INCLUDES_D}
  DESTINATION ${PROJECT_INCLUDE_DIR}
  FILES_MATCHING
  PATTERN "*.hpp"
  PATTERN "*.h"
)

install(FILES ${PROJECT_INCLUDES_F} DESTINATION ${PROJECT_INCLUDE_DIR})
