# We download some 3rdparty modules from github.com via DownloadProject
include(DownloadProject)

set(THIRD_PARTY_DIR "${PROJECT_BINARY_DIR}/3rdparty")

#--------------------------------------------------------------------------------
# the Catch framework for testing
download_project(PROJ               Catch
                GIT_REPOSITORY      https://github.com/catchorg/Catch2
                GIT_TAG             master
                UPDATE_DISCONNECTED 1
                QUIET
)

add_custom_command(
  		OUTPUT ${THIRD_PARTY_DIR}/catch
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
                ${Catch_SOURCE_DIR}/single_include/catch2/catch.hpp
                ${PROJECT_SOURCE_DIR}/test)

#--------------------------------------------------------------------------------
# the format library
download_project(PROJ               Format
                GIT_REPOSITORY      https://github.com/fmtlib/fmt.git
                GIT_TAG             master
                UPDATE_DISCONNECTED 1
                QUIET
)
add_custom_command(
        OUTPUT ${THIRD_PARTY_DIR}/fmt
        COMMAND ${CMAKE_COMMAND} -E make_directory ${THIRD_PARTY_DIR}/fmt
        COMMAND ${CMAKE_COMMAND} -E copy
                ${Format_SOURCE_DIR}/include/fmt/format.h
                ${THIRD_PARTY_DIR}/fmt
        COMMAND ${CMAKE_COMMAND} -E copy
                ${Format_SOURCE_DIR}/include/fmt/format-inl.h
                ${THIRD_PARTY_DIR}/fmt
        COMMAND ${CMAKE_COMMAND} -E copy
                ${Format_SOURCE_DIR}/include/fmt/core.h
                ${THIRD_PARTY_DIR}/fmt)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DFMT_HEADER_ONLY=1")
include_directories("${THIRD_PARTY_DIR}/fmt")

#--------------------------------------------------------------------------------
if (BUILD_GOOGLE_BENCH)
# Google Benchmark framework
download_project(PROJ               benchmark
                GIT_REPOSITORY      https://github.com/google/benchmark.git
                GIT_TAG             master
                UPDATE_DISCONNECTED 1
                QUIET
)
download_project(PROJ               googletest
                GIT_REPOSITORY      https://github.com/google/googletest.git
                GIT_TAG             master
                UPDATE_DISCONNECTED 1
                QUIET
)

add_custom_command(
	    OUTPUT ${THIRD_PARTY_DIR}/benchmark
	    COMMAND ${CMAKE_COMMAND} -E copy_directory
	            ${googletest_SOURCE_DIR}
	            ${benchmark_SOURCE_DIR}/googletest
	    COMMAND ${CMAKE_COMMAND} -E chdir ${benchmark_SOURCE_DIR} cmake -DCMAKE_BUILD_TYPE=Release
		COMMAND ${CMAKE_COMMAND} -E chdir ${benchmark_SOURCE_DIR} $(MAKE)
	    COMMAND ${CMAKE_COMMAND} -E make_directory ${THIRD_PARTY_DIR}/benchmark/include
	    COMMAND ${CMAKE_COMMAND} -E make_directory ${THIRD_PARTY_DIR}/benchmark/lib
	    COMMAND ${CMAKE_COMMAND} -E copy_directory
	            ${benchmark_SOURCE_DIR}/include
	            ${THIRD_PARTY_DIR}/benchmark/include
	    COMMAND ${CMAKE_COMMAND} -E copy
	            ${benchmark_SOURCE_DIR}/src/libbenchmark.a
	            ${THIRD_PARTY_DIR}/benchmark/lib
)

endif()

#--------------------------------------------------------------------------------
# Peristent Memory Development Kit (pmem.io)
download_project(PROJ               pmdk
                GIT_REPOSITORY      https://github.com/pmem/pmdk.git
                GIT_TAG             master
                UPDATE_DISCONNECTED 1
                QUIET
)
add_custom_command(
        OUTPUT ${THIRD_PARTY_DIR}/pmdk
        COMMAND ${CMAKE_COMMAND} -E chdir ${pmdk_SOURCE_DIR} $(MAKE)
	    COMMAND ${CMAKE_COMMAND} -E chdir ${pmdk_SOURCE_DIR} $(MAKE) install prefix=${THIRD_PARTY_DIR}/pmdk
)

###
download_project(PROJ               pmdk-cpp
                GIT_REPOSITORY      https://github.com/pmem/libpmemobj-cpp
                GIT_TAG             master
                UPDATE_DISCONNECTED 1
                QUIET
)
add_custom_command(
        OUTPUT ${THIRD_PARTY_DIR}/pmdk-cpp
        #        COMMAND ${CMAKE_COMMAND} -E make_directory ${pmdk-cpp_SOURCE_DIR}/build
        #COMMAND ${CMAKE_COMMAND} -E chdir ${pmdk-cpp_SOURCE_DIR}/build cmake .. -DCMAKE_PREFIX_PATH=${THIRD_PARTY_DIR}/pmdk -DCMAKE_INSTALL_PREFIX=${THIRD_PARTY_DIR}/pmdk
        #COMMAND ${CMAKE_COMMAND} -E chdir ${pmdk-cpp_SOURCE_DIR}/build $(MAKE)
        #COMMAND ${CMAKE_COMMAND} -E chdir ${pmdk-cpp_SOURCE_DIR}/build $(MAKE) install
	    COMMAND ${CMAKE_COMMAND} -E copy_directory
	            ${pmdk-cpp_SOURCE_DIR}/include
	            ${THIRD_PARTY_DIR}/pmdk/include
)
