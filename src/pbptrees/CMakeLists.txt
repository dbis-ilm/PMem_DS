project (pbptree)

include_directories(${PROJECT_SOURCE_DIR})
get_property(I_DIRS DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} PROPERTY INCLUDE_DIRECTORIES)
set(TEST_INCLUDE_DIRS ${TEST_INCLUDE_DIRS}
                      ${I_DIRS}
  CACHE INTERNAL "TESTING: Include Directories" FORCE
)

################
# Installation #
################
#
set(PROJECT_INCLUDES_F ${PROJECT_INCLUDES_F}
                       ${CMAKE_CURRENT_SOURCE_DIR}/PBPTree.hpp
                       ${CMAKE_CURRENT_SOURCE_DIR}/UnsortedPBPTree.hpp
                       ${CMAKE_CURRENT_SOURCE_DIR}/BitPBPTree.hpp
                       ${CMAKE_CURRENT_SOURCE_DIR}/PTree.hpp
                       ${CMAKE_CURRENT_SOURCE_DIR}/FPTree.hpp
                       ${CMAKE_CURRENT_SOURCE_DIR}/wBPTree.hpp
                       ${CMAKE_CURRENT_SOURCE_DIR}/wHBPTree.hpp
                       PARENT_SCOPE)
