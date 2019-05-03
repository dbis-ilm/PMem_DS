project (ptable)

set (PTable_VERSION_MAJOR 0)
set (PTable_VERSION_MINOR 1)

###########################
# Building PTable library #
###########################
#
include_directories(${PROJECT_SOURCE_DIR}
                    ${PBPTREE_INCLUDE_DIRS})
get_property(I_DIRS DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} PROPERTY INCLUDE_DIRECTORIES)
set(TEST_INCLUDE_DIRS ${TEST_INCLUDE_DIRS}
                      ${I_DIRS}
  CACHE INTERNAL "TESTING: Include Directories" FORCE
)

set(ptable_sources core/PTableInfo.cpp)

add_library(ptable SHARED ${ptable_sources})
target_link_libraries(ptable nvmDS_deps)

#get_property(dirs DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} PROPERTY INCLUDE_DIRECTORIES)
#foreach(dir ${dirs})
#  message(STATUS "dir='${dir}'")
#endforeach()

################
# Installation #
################
#
install(TARGETS ptable DESTINATION ${PROJECT_LIB_DIR})
set(PROJECT_INCLUDES_D ${PROJECT_INCLUDES_D}
                       ${CMAKE_CURRENT_SOURCE_DIR}/core
                       PARENT_SCOPE)
set(PROJECT_INCLUDES_F ${PROJECT_INCLUDES_F}
                       ${CMAKE_CURRENT_SOURCE_DIR}/PTable.hpp
                       PARENT_SCOPE)
