include(../../cmake/Testing.cmake.in)

#=============================================
# define the test data directory for the input and output
#add_definitions(-DTEST_DATA_DIRECTORY="${CMAKE_CURRENT_SOURCE_DIR}/test_data/")

if (BUILD_TEST_CASES)
    add_library(MainTest OBJECT MainTest.cpp)
    # PBPTree
    do_test(PBPTreeTest)
    do_test(UnsortedPBPTreeTest)
    # FPTree
    do_test(FPTreeTest)
    # PTable
    do_test(VTableInfoTest ptable)
    do_test(BDCCInfoTest ptable)
    do_test(DataNodeTest ptable)
    do_test(PTableInfoTest ptable)
    do_test(PTupleTest ptable)
    do_test(PTableTest ptable)
endif ()
