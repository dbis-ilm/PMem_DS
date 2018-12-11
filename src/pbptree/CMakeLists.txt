project (pbptree)

###########################
# Building PTable library #
###########################
#
include_directories(${PROJECT_SOURCE_DIR} 
                    ${THIRD_PARTY_DIR})
get_property(I_DIRS DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} PROPERTY INCLUDE_DIRECTORIES)
set(TEST_INCLUDE_DIRS ${TEST_INCLUDE_DIRS} 
                      ${I_DIRS}
  CACHE INTERNAL "TESTING: Include Directories" FORCE
)

set(PBPTREE_INCLUDE_DIRS ${PROJECT_SOURCE_DIR})
################
# Installation #
################
#
#set(PBPTREE_INCLUDE_DIR "${PBPTREE_DIR}/include")

#install(FILES "${CMAKE_CURRENT_SOURCE_DIR}/PBPTree.hpp"
#  DESTINATION ${PBPTREE_INCLUDE_DIR}
#)
