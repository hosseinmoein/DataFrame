include_directories(${PROJECT_SOURCE_DIR}/DataFrame/include)

# List of source files.
set(${LIBRARY_TARGET_NAME}_SRC
        src/HeteroVector.cc
        src/HeteroView.cc
        src/ThreadGranularity.cc
        src/DateTime.cc
        )

# List of header files.
set(${LIBRARY_TARGET_NAME}_HDR
        include/DataFrame/VectorView.h
        include/DataFrame/HeteroVector.h
        include/DataFrame/HeteroView.h
        include/DataFrame/HeteroVector.tcc
        include/DataFrame/HeteroView.tcc
        include/DataFrame/DataFrameVisitors.h
        include/DataFrame/DataFrameOperators.h
        include/DataFrame/DataFrame.h
        include/DataFrame/DataFrame.tcc
        include/DataFrame/DataFrame_get.tcc
        include/DataFrame/DataFrame_misc.tcc
        include/DataFrame/DataFrame_opt.tcc
        include/DataFrame/DataFrame_join.tcc
        include/DataFrame/DataFrame_shift.tcc
        include/DataFrame/DataFrame_read.tcc
        include/DataFrame/DataFrame_set.tcc
        include/DataFrame/DataFrame_functors.h
        include/DataFrame/ThreadGranularity.h
        include/DataFrame/DateTime.h
        include/DataFrame/FixedSizeString.h
        include/DataFrame/dllexports/DataFrame_lib_exports.h
        )

# Build the library
#add_definitions(-D DMS_INCLUDE_SOURCE) # Need this for some of the .tcc files to be compiled
if(STATIC)
    add_library(${LIBRARY_TARGET_NAME} STATIC ${${LIBRARY_TARGET_NAME}_SRC})
else(STATIC)
    add_library(${LIBRARY_TARGET_NAME} SHARED ${${LIBRARY_TARGET_NAME}_SRC})
    add_definitions(-DHMDF_DATAFRAME_DLL_EXPORTING)
    add_definitions(-DDATAFRAME_SHARED)
endif(STATIC)

# Set two minimum target properties for the library.
# See https://cmake.org/cmake/help/latest/command/set_target_properties.html
# Properties are: 1) Library version number 2) list of corresponding public headers
set_target_properties(${LIBRARY_TARGET_NAME} PROPERTIES VERSION       ${${PROJECT_NAME}_VERSION}
        PUBLIC_HEADER "${${LIBRARY_TARGET_NAME}_HDR}")

# Specify include directories for both compilation and installation process.
# The $<INSTALL_PREFIX> generator expression is useful to ensure to create
# relocatable configuration files, see https://cmake.org/cmake/help/latest/manual/cmake-packages.7.html#creating-relocatable-packages
target_include_directories(${LIBRARY_TARGET_NAME} PUBLIC "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
        "$<INSTALL_INTERFACE:$<INSTALL_PREFIX>/${CMAKE_INSTALL_INCLUDEDIR}>")

# Specify installation targets, typology and destination folders.
install(TARGETS ${LIBRARY_TARGET_NAME}
        EXPORT  ${PROJECT_NAME}
        LIBRARY       DESTINATION "${CMAKE_INSTALL_LIBDIR}"                            COMPONENT shlib
        ARCHIVE       DESTINATION "${CMAKE_INSTALL_LIBDIR}"                            COMPONENT lib
        RUNTIME       DESTINATION "${CMAKE_INSTALL_BINDIR}"                            COMPONENT bin
        PUBLIC_HEADER DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/${LIBRARY_TARGET_NAME}" COMPONENT dev)

# Create and install CMake configuration files for your project that are
# necessary to for other projects to call find_package().
#
# Note that it is extremely important to use exactly the project name while installing configuration
# files (you can use PROJECT_NAME variable to avoid any possible error). This is required to allow
# find_package() to properly look for the installed library in system path, in particular in Windows
# when the installation is performed in the default path.
#
# install_basic_package_files() comes with many input parameters to customize the configuration
# files. The parameters used in the following call provide basic versions of CMake configuration
# files.  See install_basic_package_files() documentation found in ./cmake folder.
#
# Note that if your library depends from other libraries, you are probably
# required to used the install_basic_package_files() DEPENDENCIES option.
include(InstallBasicPackageFiles)
if (UNIX)
    install_basic_package_files(${PROJECT_NAME}
            VERSION ${${PROJECT_NAME}_VERSION}
            COMPATIBILITY AnyNewerVersion
            EXPORT ${PROJECT_NAME}
            VARS_PREFIX ${PROJECT_NAME}
            NO_CHECK_REQUIRED_COMPONENTS_MACRO)
endif (UNIX)
if (MSVC)
    install_basic_package_files(${PROJECT_NAME}
            VERSION ${${PROJECT_NAME}_VERSION}
            COMPATIBILITY AnyNewerVersion
            EXPORT ${PROJECT_NAME}
            VARS_PREFIX ${PROJECT_NAME}
            NO_CHECK_REQUIRED_COMPONENTS_MACRO)
endif(MSVC)

# Add the uninstall target
include(AddUninstallTarget)