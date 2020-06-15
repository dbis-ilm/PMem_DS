project (ptries)

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
    ${CMAKE_CURRENT_SOURCE_DIR}/BaseTrie.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/PTreeNode.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/Patricia.hpp
                       PARENT_SCOPE)
