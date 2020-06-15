cmake_minimum_required(VERSION 3.11 FATAL_ERROR)
project (nvm-DS)

################################################################################
# customization section                                                        #
################################################################################

# Set the mount path of your pmem device where the structures should be stored
if(NOT PMEM_MNT_PATH)
  set(PMEM_MNT_PATH "/mnt/pmem0/test")
endif()

# Installation path (creates include and lib within this directory)
if(NOT PROJECT_INSTALL_DIR)
  set(PROJECT_INSTALL_DIR ${CMAKE_INSTALL_PREFIX})
endif()

option(ENABLE_LOG         "enables log output for e.g. debugging"        OFF)
option(BUILD_TEST_CASES   "build tests for functionality of structures"  ON )
option(BUILD_GOOGLE_BENCH "build google benchmark"                       ON )
option(BUILD_BENCHMARKS   "build benchmarks for structures"              ON )
option(ENABLE_PCM         "enables performance counter monitor"          OFF)

################################################################################
# End of customization section                                                 #
################################################################################

if(ENABLE_LOG)
  add_definitions("-DENABLE_LOG")
endif()
if(ENABLE_PCM)
  add_definitions("-DENABLE_PCM")
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
  message(STATUS "Setting build type to 'Release' as none was specified.")
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build." FORCE)
endif()
if(CMAKE_BUILD_TYPE MATCHES Release)
  # this disables asserts for release builds
  add_definitions("-DNDEBUG")
endif()

# show used compiler
message("Using Compiler: ${CMAKE_CXX_COMPILER_ID}.")

# Add our CMake directory to CMake's module path
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake/")

# We fetch some 3rdparty modules from github.com before building the project.
include(Fetch3rdParty)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

#################################
# Pre-built common dependencies #
#################################
#
configure_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/src/config.h.in
  ${CMAKE_CURRENT_BINARY_DIR}/generated/config.h
)

include_directories(${PROJECT_SOURCE_DIR}/src
                    ${THIRD_PARTY_DIR}
                    ${CMAKE_CURRENT_BINARY_DIR}/generated
)
add_library(nvmDS_deps STATIC ${PMDK_INCLUDE_DIRS})
target_link_libraries(nvmDS_deps
    Threads::Threads
    stdc++fs
    ${PMDK_LIBRARIES} ndctl daxctl
)
set_target_properties(nvmDS_deps PROPERTIES LINKER_LANGUAGE CXX)

#################
# Install Paths #
#################
#
#set(PROJECT_LIBS ${PROJECT_LIBS})
set(PROJECT_INCLUDES_D ${PROJECT_INCLUDES_D}
  ${CMAKE_CURRENT_SOURCE_DIR}/utils)
set(PROJECT_INCLUDES_F ${PROJECT_INCLUDES_F}
  ${CMAKE_CURRENT_BINARY_DIR}/generated/config.h)

set(PROJECT_LIB_DIR ${PROJECT_INSTALL_DIR}/lib)
set(PROJECT_INCLUDE_DIR ${PROJECT_INSTALL_DIR}/include)

#############################
# Including Data Structures #
#############################
#
add_subdirectory(src/pbptrees) # Persistent versions of B⁺-Tree, contains also:
                               # - Remake of the FPTree
                               # - Remake of the wB+Tree
add_subdirectory(src/ptable)  # BDCC-based analytical table structure

add_subdirectory(src/pskiplists)
#########################
# Unit test using Catch #
#########################
#
if(BUILD_TEST_CASES)
  enable_testing()
  add_subdirectory(test)
endif()

#############################################
# Micro-benchmarking using Google Benchmark #
#############################################
#
if(BUILD_BENCHMARKS)
  add_subdirectory(bench)
endif()

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
