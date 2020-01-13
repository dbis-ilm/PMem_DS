include(../cmake/Testing.cmake.in)

#=============================================
# define the test data directory for the input and output
#add_definitions(-DTEST_DATA_DIRECTORY="${CMAKE_CURRENT_SOURCE_DIR}/test_data/")

include_directories(${CATCH_INCLUDE_DIR})
add_library(MainTest OBJECT MainTest.cpp)
target_link_libraries(MainTest INTERFACE Catch2::Catch)

# PBPTree
do_test(PBPTreeTest)
do_test(UnsortedPBPTreeTest)
do_test(BitPBPTreeTest)
do_test(BitHPBPTreeTest)
# FPTree
do_test(HPBPTreeTest)
do_test(FPTreeTest)
# wBPTree
do_test(wBPTreeTest)
do_test(wHBPTreeTest)
# PTable
do_test(VTableInfoTest ptable)
do_test(BDCCInfoTest ptable)
do_test(DataNodeTest ptable)
do_test(PTableInfoTest ptable)
do_test(PTupleTest ptable)
do_test(PTableTest ptable)
