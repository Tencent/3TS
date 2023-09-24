CMAKE_MINIMUM_REQUIRED(VERSION 2.8) 
PROJECT(dbtest)
 
set(CMAKE_BUILD_TYPE "Debug") 
set(CMAKE_CXX_FLAGS_DEBUG "$ENV{CXXFLAGS} -O0 -Wall -g -ggdb") 
set(CMAKE_CXX_FLAGS_RELEASE "$ENV{CXXFLAGS} -O3 -Wall") 
set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR})
message("cmake moudle ${CMAKE_MODULE_PATH}")
 
set(CMAKE_CXX_FLAGS "-std=c++17 ${CMAKE_CXX_FLAGS} -g -ftest-coverage -fprofile-arcs -Wno-deprecated -fopenmp -pthread -lpthread") 

# all .h file
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src)

# ld:pthread gflags
list(APPEND THIRD_PARTIES pthread)

find_package(gflags REQUIRED)
list(APPEND THIRD_PARTIES gflags)

########### sql test V1 #############
# static test with 33 test case
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
find_package(ODBC REQUIRED)
list(APPEND ODBC_LD odbc)
# link ld
target_link_libraries(3ts_dbtest ${THIRD_PARTIES})
target_link_libraries(3ts_dbtest ${ODBC_LD})

########### sql test V2 #############
# dynamic test with auto-gen test case
# all .cc files
list(APPEND SOURCE_FILES_SQL_v2
     "${CMAKE_CURRENT_SOURCE_DIR}/src/common.cc"
     "${CMAKE_CURRENT_SOURCE_DIR}/src/case_cntl_v2.cc"
     "${CMAKE_CURRENT_SOURCE_DIR}/src/sql_cntl_v2.cc"
     "${CMAKE_CURRENT_SOURCE_DIR}/src/sqltest_v2.cc"
)
# bin
add_executable(3ts_dbtest_v2 ${SOURCE_FILES_SQL_v2})
# ld:odbc
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake/Modules/")
find_package(ODBC REQUIRED)
list(APPEND ODBC_LD odbc)
# link ld
target_link_libraries(3ts_dbtest_v2 ${THIRD_PARTIES})
target_link_libraries(3ts_dbtest_v2 ${ODBC_LD})


########### kv test ##############
# all .cc files
#list(APPEND SOURCE_FILES_KV
#     "${CMAKE_CURRENT_SOURCE_DIR}/src/common.cc"
#     "${CMAKE_CURRENT_SOURCE_DIR}/src/case_cntl.cc"
#     "${CMAKE_CURRENT_SOURCE_DIR}/src/kv_cntl.cc"
#     "${CMAKE_CURRENT_SOURCE_DIR}/src/kvtest.cc"
#)
# bin
#add_executable(3ts_kvtest ${SOURCE_FILES_KV})
# ld:bson mongo
#find_package(bsoncxx REQUIRED)
#find_package(mongocxx REQUIRED)
#include_directories(${LIBMONGOCXX_INCLUDE_DIR})
#include_directories(${LIBBSONCXX_INCLUDE_DIR})
# link ld
#target_link_libraries(3ts_kvtest PRIVATE mongo::bsoncxx_shared)
#target_link_libraries(3ts_kvtest PRIVATE mongo::mongocxx_shared)
#target_link_libraries(3ts_kvtest PRIVATE ${THIRD_PARTIES})
# third parties .h
#target_include_directories(3ts_kvtest PUBLIC
#	                       "/usr/local/include/mongocxx/v_noabi"
#			               "/usr/local/include/bsoncxx/v_noabi"
#			               "/usr/local/include/libmongoc-1.0"
#			               "/usr/local/include/libbson-1.0"
#			               "/usr/local/lib"
#			               )
