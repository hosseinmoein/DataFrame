cmake_minimum_required (VERSION 3.5)
project (DataFrame VERSION 1.0.0)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON) # Produce cmpile_commands.json
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

option(ENABLE_TESTING "Enable testing" ON)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

# Control where libraries and executables are placed during the build.
# With the following settings executables are placed in <the top level of the
# build tree>/bin and libraries/archives in <top level of the build tree>/lib.
# This is particularly useful to run ctests on libraries built on Windows
# machines: tests, which are executables, are placed in the same folders of
# dlls, which are treated as executables as well, so that they can properly
# find the libraries to run. This is a because of missing RPATH on Windows.
#
include(GNUInstallDirs)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY
    "${CMAKE_BINARY_DIR}/${CMAKE_INSTALL_BINDIR}")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY
    "${CMAKE_BINARY_DIR}/${CMAKE_INSTALL_LIBDIR}")
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY
    "${CMAKE_BINARY_DIR}/${CMAKE_INSTALL_LIBDIR}")

# To build shared libraries in Windows,
# we set CMAKE_WINDOWS_EXPORT_ALL_SYMBOLS to TRUE.
# See https://cmake.org/cmake/help/v3.4/variable/
#         CMAKE_WINDOWS_EXPORT_ALL_SYMBOLS.html
# See https://blog.kitware.com/
#   create-dlls-on-windows-without-declspec-using-new-cmake-export-all-feature/
#
## set(CMAKE_WINDOWS_EXPORT_ALL_SYMBOLS ON)

# Disable C and C++ compiler extensions. C/CXX_EXTENSIONS are ON by default to
# allow the compilers to use extended variants of the C/CXX language. However,
# this could expose cross-platform bugs in user code or in the headers of
# third-party dependencies and thus it is strongly suggested to turn
# extensions off.
#
set(CMAKE_C_EXTENSIONS OFF)
set(CMAKE_CXX_EXTENSIONS OFF)

# Enable RPATH support for installed binaries and libraries
#
include(AddInstallRPATHSupport)
add_install_rpath_support(BIN_DIRS "${CMAKE_INSTALL_FULL_LIBDIR}"
                          LIB_DIRS "${CMAKE_INSTALL_FULL_BINDIR}"
                          INSTALL_NAME_DIR "${CMAKE_INSTALL_FULL_LIBDIR}"
                          USE_LINK_PATH)

Include(CheckSymbolExists)
check_symbol_exists(clock_gettime "time.h" HMDF_HAVE_CLOCK_GETTIME)

# Encourage user to specify a build type (e.g. Release, Debug, etc.),
# otherwise set it to Release.
#
if(NOT CMAKE_CONFIGURATION_TYPES)
    if(NOT CMAKE_BUILD_TYPE)
        message(STATUS "Setting build type to 'Release' as none was specified.")
        set_property(CACHE CMAKE_BUILD_TYPE PROPERTY VALUE "Release")
    endif()
endif()

set(LIBRARY_TARGET_NAME ${PROJECT_NAME})

# List of source files.
#
set(${LIBRARY_TARGET_NAME}_SRC
    src/Vectors/HeteroVector.cc
    src/Vectors/HeteroView.cc
    src/Vectors/HeteroPtrView.cc
    src/Utils/DateTime.cc
)

# List of header files.
#
set(${LIBRARY_TARGET_NAME}_HDR
    include/DataFrame/Vectors/VectorView.h
    include/DataFrame/Vectors/VectorPtrView.h
    include/DataFrame/Vectors/HeteroVector.h
    include/DataFrame/Vectors/HeteroView.h
    include/DataFrame/Vectors/HeteroPtrView.h
    include/DataFrame/Vectors/HeteroVector.tcc
    include/DataFrame/Vectors/HeteroView.tcc
    include/DataFrame/Vectors/HeteroPtrView.tcc
    include/DataFrame/DataFrameStatsVisitors.h
    include/DataFrame/DataFrameMLVisitors.h
    include/DataFrame/DataFrameFinancialVisitors.h
    include/DataFrame/DataFrameTransformVisitors.h
    include/DataFrame/DataFrameTypes.h
    include/DataFrame/RandGen.h
    include/DataFrame/DataFrameOperators.h
    include/DataFrame/DataFrame.h
    include/DataFrame/Internals/DataFrame.tcc
    include/DataFrame/Internals/DataFrame_get.tcc
    include/DataFrame/Internals/DataFrame_visit.tcc
    include/DataFrame/Internals/DataFrame_misc.tcc
    include/DataFrame/Internals/DataFrame_standalone.tcc
    include/DataFrame/Internals/DataFrame_opt.tcc
    include/DataFrame/Internals/DataFrame_join.tcc
    include/DataFrame/Internals/DataFrame_shift.tcc
    include/DataFrame/Internals/DataFrame_read.tcc
    include/DataFrame/Internals/DataFrame_write.tcc
    include/DataFrame/Internals/DataFrame_set.tcc
    include/DataFrame/Internals/DataFrame_private_decl.h
    include/DataFrame/Internals/DataFrame_functors.h
    include/DataFrame/Internals/RandGen.tcc
    include/DataFrame/Utils/ThreadGranularity.h
    include/DataFrame/Utils/DateTime.h
    include/DataFrame/Utils/Utils.h
    include/DataFrame/Utils/FixedSizeString.h
    include/DataFrame/Utils/FixedSizePriorityQueue.h
)

# Build the library
#
if (UNIX)
    add_library(${LIBRARY_TARGET_NAME} ${${LIBRARY_TARGET_NAME}_SRC})
endif(UNIX)

if (MSVC)
    add_definitions(-D_CRT_SECURE_NO_WARNINGS)
    add_definitions(-D_USE_MATH_DEFINES)
    add_library(${LIBRARY_TARGET_NAME} ${${LIBRARY_TARGET_NAME}_SRC})

    # If shared:
        # At build time: LIBRARY_EXPORTS and HMDF_SHARED defined
        # At consume time: HMDF_SHARED defined
    # If static: no definition at build and consume time
    #
    add_definitions(-DLIBRARY_EXPORTS)
    add_definitions(-DHMDF_SHARED)
endif(MSVC)

if (MINGW)
    add_library(${LIBRARY_TARGET_NAME} ${${LIBRARY_TARGET_NAME}_SRC})
endif()

# Set two minimum target properties for the library.
# See https://cmake.org/cmake/help/latest/command/set_target_properties.html
# Properties are: 1) Library version number 2) list of corresponding public
# headers
#
set_target_properties(${LIBRARY_TARGET_NAME}
                      PROPERTIES VERSION ${${PROJECT_NAME}_VERSION})

# Specify include directories for both compilation and installation process.
# The $<INSTALL_PREFIX> generator expression is useful to ensure to create
# relocatable configuration files,
# see https://cmake.org/cmake/help/latest/manual/
#         cmake-packages.7.html#creating-relocatable-packages
#
target_include_directories(${LIBRARY_TARGET_NAME}
    PUBLIC "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
    "$<INSTALL_INTERFACE:$<INSTALL_PREFIX>/${CMAKE_INSTALL_INCLUDEDIR}>")

# Specify installation targets, typology and destination folders.
#
install(TARGETS ${LIBRARY_TARGET_NAME}
        EXPORT  ${PROJECT_NAME}
        LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}" COMPONENT lib
        ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}" COMPONENT lib
        RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}" COMPONENT bin
)

install(DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/include/${PROJECT_NAME}" # src
        DESTINATION "include"  # target directory
        COMPONENT dev
)

# Support pkg-config
#
set(PROJECT_NAME_VERSION "${${PROJECT_NAME}_VERSION}")
configure_file(${PROJECT_NAME}.pc.in
               "${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}.pc"
               @ONLY)

install(FILES "${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}.pc"
        DESTINATION "${CMAKE_INSTALL_LIBDIR}/pkgconfig/")

# Create and install CMake configuration files for your project that are
# necessary to for other projects to call find_package().
#
# Note that it is extremely important to use exactly the project name while
# installing configurationfiles (you can use PROJECT_NAME variable to avoid
# any possible error). This is required to allow find_package() to properly
# look for the installed library in system path, in particular in Windows
# when the installation is performed in the default path.
#
# install_basic_package_files() comes with many input parameters to
# customize the configuration files. The parameters used in the following call
# provide basic versions of CMake configuration files.
# See install_basic_package_files() documentation found in ./cmake folder.
#
# Note that if your library depends from other libraries, you are probably
# required to used the install_basic_package_files() DEPENDENCIES option.
#
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
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /bigobj /wd4251")
endif(MSVC)

# Add the uninstall target if its not defined
#
if(NOT TARGET uninstall)
    include(AddUninstallTarget)
endif()

if (ENABLE_TESTING)
    # Build the test binary
    #
    add_executable(dataframe_tester  test/dataframe_tester.cc)
    add_executable(dataframe_tester_2  test/dataframe_tester_2.cc)
    add_executable(dataframe_tester_3  test/dataframe_tester_3.cc)
    add_executable(hello_world  test/hello_world.cc)
    add_executable(dataframe_performance  test/dataframe_performance.cc)
    add_executable(dataframe_performance_2  test/dataframe_performance_2.cc)
    add_executable(vectors_tester    test/vectors_tester.cc)
    add_executable(vector_ptr_view_tester test/vector_ptr_view_tester.cc)
    add_executable(date_time_tester  test/date_time_tester.cc)
    add_executable(gen_rand_tester  test/gen_rand_tester.cc)
    add_executable(dataframe_thread_safety  test/dataframe_thread_safety.cc)

    # Link the DataFrame library to the test binary
    #
    target_link_libraries(dataframe_tester DataFrame)
    target_link_libraries(dataframe_tester_2 DataFrame)
    target_link_libraries(dataframe_tester_3 DataFrame)
    target_link_libraries(hello_world DataFrame)
    target_link_libraries(dataframe_performance DataFrame)
    target_link_libraries(dataframe_performance_2 DataFrame)
    target_link_libraries(vectors_tester DataFrame)
    target_link_libraries(vector_ptr_view_tester DataFrame)
    target_link_libraries(date_time_tester DataFrame)
    target_link_libraries(gen_rand_tester DataFrame)
    target_link_libraries(dataframe_thread_safety DataFrame)

    if (UNIX)
        # Find pthreads library
        #
        set(THREADS_PREFER_PTHREAD_FLAG ON)
        find_package(Threads REQUIRED)
        target_link_libraries(dataframe_tester Threads::Threads)
        target_link_libraries(dataframe_tester_2 Threads::Threads)
        target_link_libraries(dataframe_tester_3 Threads::Threads)
        target_link_libraries(hello_world Threads::Threads)
        target_link_libraries(dataframe_performance Threads::Threads)
        target_link_libraries(dataframe_performance_2 Threads::Threads)
        target_link_libraries(vectors_tester Threads::Threads)
        target_link_libraries(vector_ptr_view_tester Threads::Threads)
        target_link_libraries(date_time_tester Threads::Threads)
        target_link_libraries(gen_rand_tester Threads::Threads)
        target_link_libraries(dataframe_thread_safety Threads::Threads)
    endif (UNIX)

    if (UNIX AND NOT APPLE)
        target_link_libraries(dataframe_tester rt)
        target_link_libraries(dataframe_tester_2 rt)
        target_link_libraries(dataframe_tester_3 rt)
        target_link_libraries(hello_world rt)
        target_link_libraries(dataframe_performance rt)
        target_link_libraries(dataframe_performance_2 rt)
        target_link_libraries(vectors_tester rt)
        target_link_libraries(vector_ptr_view_tester rt)
        target_link_libraries(date_time_tester rt)
        target_link_libraries(gen_rand_tester rt)
        target_link_libraries(dataframe_thread_safety rt)
    endif()

    # Enable ctest, testing so we can see if unit tests pass or fail in CI
    #
    enable_testing()
    add_test(NAME dataframe_tester
             COMMAND dataframe_tester
             WORKING_DIRECTORY $<TARGET_FILE_DIR:dataframe_tester>)

    # For some unknown reason to me, these tests sigfaults in AppVeyor
    #
    if (NOT MSVC)
        add_test(NAME dataframe_tester_2
                 COMMAND dataframe_tester_2
                 WORKING_DIRECTORY $<TARGET_FILE_DIR:dataframe_tester_2>)

        add_test(NAME vectors_tester
                 COMMAND vectors_tester
                 WORKING_DIRECTORY $<TARGET_FILE_DIR:vectors_tester>)
    endif(NOT MSVC)

    add_test(NAME vector_ptr_view_tester
             COMMAND vector_ptr_view_tester
             WORKING_DIRECTORY $<TARGET_FILE_DIR:vector_ptr_view_tester>)
    add_test(NAME date_time_tester
             COMMAND date_time_tester
             WORKING_DIRECTORY $<TARGET_FILE_DIR:date_time_tester>)

    add_test(NAME gen_rand_tester
             COMMAND gen_rand_tester
             WORKING_DIRECTORY $<TARGET_FILE_DIR:gen_rand_tester>)

    add_test(NAME dataframe_thread_safety
             COMMAND dataframe_thread_safety
             WORKING_DIRECTORY $<TARGET_FILE_DIR:dataframe_thread_safety>)

    message("-- Copying files for testing")
    # Ctest require this files in the build dir, on all platforms
    #
    add_custom_command(TARGET ${PROJECT_NAME} POST_BUILD
                       COMMAND ${CMAKE_COMMAND} -E copy
                       ${CMAKE_CURRENT_SOURCE_DIR}/test/data/sample_data.csv
                       ${CMAKE_CURRENT_BINARY_DIR}/data/sample_data.csv)
    add_custom_command(TARGET ${PROJECT_NAME} POST_BUILD
                       COMMAND ${CMAKE_COMMAND} -E copy
                       ${CMAKE_CURRENT_SOURCE_DIR}/test/data/sample_data.json
                       ${CMAKE_CURRENT_BINARY_DIR}/data/sample_data.json)
    add_custom_command(TARGET ${PROJECT_NAME} POST_BUILD
                       COMMAND ${CMAKE_COMMAND} -E copy
                       ${CMAKE_CURRENT_SOURCE_DIR}/test/data/csv2_format_data.csv
                       ${CMAKE_CURRENT_BINARY_DIR}/data/csv2_format_data.csv)
    add_custom_command(TARGET ${PROJECT_NAME} POST_BUILD
                       COMMAND ${CMAKE_COMMAND} -E copy
                       ${CMAKE_CURRENT_SOURCE_DIR}/test/data/SHORT_IBM.csv
                       ${CMAKE_CURRENT_BINARY_DIR}/data/SHORT_IBM.csv)
    add_custom_command(TARGET ${PROJECT_NAME} POST_BUILD
                       COMMAND ${CMAKE_COMMAND} -E copy
                       ${CMAKE_CURRENT_SOURCE_DIR}/test/data/sample_data_dt_index.csv
                       ${CMAKE_CURRENT_BINARY_DIR}/data/sample_data_dt_index.csv)
    add_custom_command(TARGET ${PROJECT_NAME} POST_BUILD
                       COMMAND ${CMAKE_COMMAND} -E copy
                       ${CMAKE_CURRENT_SOURCE_DIR}/test/data/sample_data_string_index.csv
                       ${CMAKE_CURRENT_BINARY_DIR}/data/sample_data_string_index.csv)
   

    file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/test/data/sample_data.csv
              DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/bin/Debug/data/.)
    file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/test/data/sample_data.csv
              DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/bin/Release/data/.)
    file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/test/data/sample_data.json
              DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/bin/Debug/data/.)
    file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/test/data/sample_data.json
              DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/bin/Release/data/.)
    file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/test/data/sample_data_dt_index.csv
              DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/bin/Debug/data/.)
    file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/test/data/sample_data_dt_index.csv
              DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/bin/Release/data/.)
    file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/test/data/sample_data_string_index.csv
              DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/bin/Debug/data/.)
    file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/test/data/sample_data_string_index.csv
              DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/bin/Release/data/.)
    file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/test/data/csv2_format_data.csv
              DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/bin/Debug/data/.)
    file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/test/data/csv2_format_data.csv
              DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/bin/Release/data/.)
     file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/test/data/SHORT_IBM.csv
              DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/bin/Debug/data/.)
     file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/test/data/SHORT_IBM.csv
              DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/bin/Release/data/.)
    message("-- Copying files for testing - done")
endif(ENABLE_TESTING)
