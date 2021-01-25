# - Find libconfig
# - This module determines the libconfig library of the system
# The following variables are set if the library found:
# LIBCONFIG_FOUND - If false do nnt try to use libconfig.
# LIBCONFIG_INCLUDE_DIRS - where to find the headfile of library.
# LIBCONFIG_LIBRARY_DIRS - where to find the libconfig library.
# LIBCONFIG_LIBRARIES, the library file name needed to use libconfig.
# LIBCONFIG_LIBRARY - the library needed to use libconfig.
# imported target
#   libconfig::libconfig

if(LIBCONFIG_FOUND)
	return()
endif()
if (WIN32)
	# windows下使用CONFIG模式调用find_package查找
	find_package(LIBCONFIG CONFIG)
else ()
	# linux下调用pkg_check_modules 查找
    include(FindPkgConfig)
    unset(_verexp)
    if(LIBCONFIG_FIND_VERSION)
    	if(LIBCONFIG_FIND_VERSION_EXACT)    	
    	    set(_verexp "=${LIBCONFIG_FIND_VERSION}")
    	else()
    	    set(_verexp ">=${LIBCONFIG_FIND_VERSION}")
    	endif()
    endif()
    pkg_check_modules (LIBCONFIG libconfig${_verexp})
endif()

# handle the QUIETLY and REQUIRED arguments and set LIBCONFIG_FOUND to TRUE if
# all listed variables are TRUE
include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(LIBCONFIG DEFAULT_MSG	LIBCONFIG_LIBRARIES LIBCONFIG_INCLUDE_DIRS)

if(LIBCONFIG_FOUND)
    if(NOT LIBCONFIG_FIND_QUIETLY)
        message(STATUS "  -I: ${LIBCONFIG_INCLUDE_DIRS}")
        message(STATUS "  -L: ${LIBCONFIG_LIBRARY_DIRS}")
        message(STATUS "  -l: ${LIBCONFIG_LIBRARIES}")
    endif()    
    find_library (LIBCONFIG_LIBRARY NAMES ${LIBCONFIG_LIBRARIES} PATHS ${LIBCONFIG_LIBRARY_DIRS})

    # 创建 imported target
    if (NOT TARGET libconfig::libconfig)
        add_library(libconfig::libconfig INTERFACE IMPORTED)
        set_target_properties(libconfig::libconfig PROPERTIES
        	INTERFACE_COMPILE_OPTIONS "${LIBCONFIG_CFLAGS}"
            INTERFACE_INCLUDE_DIRECTORIES "${LIBCONFIG_INCLUDE_DIRS}"
    		INTERFACE_LINK_LIBRARIES   "-L${LIBCONFIG_LIBRARY_DIRS} -l${LIBCONFIG_LIBRARIES }"
	    	)
	    if(NOT LIBCONFIG_FIND_QUIETLY)
    	    message(STATUS "IMPORTED TARGET: libconfig::libconfig,link libraies ${_link_libs}")
    	endif()
    endif ()
endif(LIBCONFIG_FOUND)
