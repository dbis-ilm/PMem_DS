project (ptable)

set (PTable_VERSION_MAJOR 0)
set (PTable_VERSION_MINOR 1)

################################################################################
# customization section                                                        #
################################################################################

# Installation path
if(NOT PTABLE_DIR)
  set(PTABLE_DIR "/usr/local/ptable")
endif()

################################################################################
# End of customization section                                                 #
################################################################################


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
target_link_libraries(ptable dependencies)

#get_property(dirs DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} PROPERTY INCLUDE_DIRECTORIES)
#foreach(dir ${dirs})
#  message(STATUS "dir='${dir}'")
#endforeach()

################
# Installation #
################
#
set(PTABLE_LIBS ptable)
set(PTABLE_LIB_DIR "${PTABLE_DIR}/lib")
set(PTABLE_INCLUDE_DIR "${PTABLE_DIR}/include")

foreach(LIB ${PTABLE_LIBS})
install(TARGETS ${LIB} DESTINATION ${PTABLE_LIB_DIR})
endforeach(LIB)

install(DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/core"
                  "${CMAKE_BINARY_DIR}/generated/"
   DESTINATION ${PTABLE_INCLUDE_DIR}
   FILES_MATCHING
   PATTERN "*.hpp"
   PATTERN "*.h"
)

install(FILES "${CMAKE_CURRENT_SOURCE_DIR}/PBPTree.hpp"
              "${CMAKE_CURRENT_SOURCE_DIR}/PTable.hpp"
  DESTINATION ${PTABLE_INCLUDE_DIR}
)
