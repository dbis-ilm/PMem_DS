message("===== Fetching 3rd Party Dependencies ================================")
include(FetchContent REQUIRED)
find_package(PkgConfig REQUIRED)

set(THIRD_PARTY_DIR "${PROJECT_BINARY_DIR}/_deps")

# Searching for PMDK ======================================================== #
message(STATUS "Searching for PMDK")
pkg_check_modules(PMDK QUIET REQUIRED libpmemobj>=1.5)
mark_as_advanced(PMDK_LIBRARIES PMDK_INCLUDE_DIRS)
  
# Format ==================================================================== #
FetchContent_Declare(
  fmt
  GIT_REPOSITORY https://github.com/fmtlib/fmt.git
  GIT_TAG        5.3.0
  )
FetchContent_GetProperties(fmt)
if(NOT fmt_POPULATED)
  message(STATUS "Populating fmt (Format)")
  FetchContent_Populate(fmt)
  add_subdirectory(${fmt_SOURCE_DIR} ${fmt_BINARY_DIR} EXCLUDE_FROM_ALL)
endif()



if(BUILD_TEST_CASES)
  # Catch =================================================================== #
  FetchContent_Declare(
    catch
    GIT_REPOSITORY https://github.com/catchorg/Catch2.git
    GIT_TAG        v2.9.1
    )
  FetchContent_GetProperties(catch)
  if(NOT catch_POPULATED)
    message(STATUS "Populating catch")
    FetchContent_Populate(catch)
    set(CATCH_INCLUDE_DIR ${catch_SOURCE_DIR}/single_include/catch2)
    add_library(Catch2::Catch IMPORTED INTERFACE)
    set_property(TARGET Catch2::Catch PROPERTY INTERFACE_INCLUDE_DIRECTORIES "${CATCH_INCLUDE_DIR}")
  endif()
endif()



if(BUILD_BENCHMARKS)
  # Google Test ============================================================= #
  #[[
  FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG        release-1.8.1
    GIT_SHALLOW    TRUE
  )
  FetchContent_GetProperties(googletest)
  if(NOT googletest_POPULATED)
    message(STATUS "Populating googletest")
    FetchContent_Populate(googletest)
    # add_subdirectory(${googletest_SOURCE_DIR} ${googletest_BINARY_DIR} EXCLUDE_FROM_ALL)
  endif()
  ]]
  # Google Benchmark ======================================================== #
  FetchContent_Declare(
    benchmark
    GIT_REPOSITORY https://github.com/google/benchmark.git
    GIT_TAG        v1.5.0
    GIT_SHALLOW    TRUE
    )
  FetchContent_GetProperties(benchmark)
  if(NOT benchmark_POPULATED)
    set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
    set(BENCHMARK_ENABLE_GTEST_TESTS OFF CACHE BOOL "" FORCE)
    message(STATUS "Populating benchmark (google)")
    FetchContent_Populate(benchmark) 
    add_subdirectory(${benchmark_SOURCE_DIR} ${benchmark_BINARY_DIR} EXCLUDE_FROM_ALL)
  endif()
endif()

message("===== Finished fetching 3rd Party Dependencies =======================")
