project (fptree)

include_directories(${PROJECT_SOURCE_DIR}
                    ${THIRD_PARTY_DIR})
get_property(I_DIRS DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} PROPERTY INCLUDE_DIRECTORIES)
set(TEST_INCLUDE_DIRS ${TEST_INCLUDE_DIRS}
                      ${I_DIRS}
  CACHE INTERNAL "TESTING: Include Directories" FORCE
)

set(PROJECT_INCLUDES_F ${PROJECT_INCLUDES_F}
                       ${CMAKE_CURRENT_SOURCE_DIR}/PTree.hpp
                       ${CMAKE_CURRENT_SOURCE_DIR}/FPTree.hpp
                       PARENT_SCOPE)
