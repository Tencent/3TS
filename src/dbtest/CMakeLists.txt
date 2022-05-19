CMAKE_MINIMUM_REQUIRED(VERSION 2.8) 
PROJECT(dbtest)
 
set(CMAKE_BUILD_TYPE "Debug") 
set(CMAKE_CXX_FLAGS_DEBUG "$ENV{CXXFLAGS} -O0 -Wall -g -ggdb") 
set(CMAKE_CXX_FLAGS_RELEASE "$ENV{CXXFLAGS} -O3 -Wall") 
message("cmake moudle ${CMAKE_MODULE_PATH}")
 
set(CMAKE_CXX_FLAGS "-std=c++17 ${CMAKE_CXX_FLAGS} -g -ftest-coverage -fprofile-arcs -Wno-deprecated") 

# all .h file
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src)

# ld:pthread gflags
list(APPEND THIRD_PARTIES pthread)

find_package(gflags REQUIRED)
list(APPEND THIRD_PARTIES gflags)

########### sql test #############
# all .cc files
list(APPEND SOURCE_FILES_SQL
     "${CMAKE_CURRENT_SOURCE_DIR}/src/common.cc"
     "${CMAKE_CURRENT_SOURCE_DIR}/src/case_cntl.cc"
     "${CMAKE_CURRENT_SOURCE_DIR}/src/sql_cntl.cc"
     "${CMAKE_CURRENT_SOURCE_DIR}/src/sqltest.cc"
)
# bin
add_executable(3ts_dbtest ${SOURCE_FILES_SQL})
# ld:odbc
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake/Modules/")
find_package(odbc REQUIRED)
list(APPEND ODBC_LD odbc)
# link ld
target_link_libraries(3ts_dbtest ${THIRD_PARTIES})
target_link_libraries(3ts_dbtest ${ODBC_LD})

