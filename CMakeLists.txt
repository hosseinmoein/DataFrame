cmake_minimum_required(VERSION 3.8)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(POLICY CMP0090)
    cmake_policy(SET CMP0090 NEW)
endif()

project(DataFrame VERSION 3.0.0 LANGUAGES CXX)

option(HMDF_TESTING "Enable testing" OFF)
option(HMDF_EXAMPLES "Build Examples" OFF)
option(HMDF_BENCHMARKS "Build Benchmarks" OFF)

if(HMDF_TESTING)
    enable_testing()
endif()

if(HMDF_SANITY_EXCEPTIONS)
    add_compile_definitions(HMDF_SANITY_EXCEPTIONS)
endif()

list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/bin)

include(CheckSymbolExists)
include(CMakePackageConfigHelpers)
include(GNUInstallDirs)

file(GLOB DATA_FILES ${PROJECT_SOURCE_DIR}/data/*.*)
file(COPY ${DATA_FILES} DESTINATION ${CMAKE_RUNTIME_OUTPUT_DIRECTORY})

# Build DataFrame library
check_symbol_exists(clock_gettime "time.h" HMDF_HAVE_CLOCK_GETTIME)

find_package(Threads REQUIRED)

add_library(DataFrame)
add_library(DataFrame::DataFrame ALIAS DataFrame)

target_sources(DataFrame PRIVATE src/Utils/DateTime.cc)

target_compile_features(DataFrame PUBLIC cxx_std_23)
target_compile_definitions(
    DataFrame
    PRIVATE $<$<BOOL:${HMDF_HAVE_CLOCK_GETTIME}>:HMDF_HAVE_CLOCK_GETTIME>
    PUBLIC $<$<CXX_COMPILER_ID:MSVC>:_USE_MATH_DEFINES>
)
target_compile_options(
    DataFrame
    PRIVATE $<$<CXX_COMPILER_ID:MSVC>:/bigobj>
            $<$<CXX_COMPILER_ID:MSVC>:/wd4251>
    # TODO: fix these warnings
    PUBLIC $<$<CXX_COMPILER_ID:MSVC>:/wd4244>
           $<$<CXX_COMPILER_ID:MSVC>:/wd4267>
           $<$<CXX_COMPILER_ID:MSVC>:/wd4996>
)
set_target_properties(
    DataFrame
    PROPERTIES VERSION ${PROJECT_VERSION} SOVERSION ${PROJECT_VERSION_MAJOR}
)

if(BUILD_SHARED_LIBS)
    set_target_properties(
        DataFrame
        PROPERTIES CXX_VISIBILITY_PRESET
        hidden VISIBILITY_INLINES_HIDDEN ON
    )
    target_compile_definitions(DataFrame PUBLIC HMDF_SHARED)
endif()

target_link_libraries(
    DataFrame
    INTERFACE Threads::Threads
              $<$<AND:$<BOOL:${UNIX}>,$<NOT:$<BOOL:${APPLE}>>>:rt>
)

target_include_directories(
    DataFrame
    PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
           $<INSTALL_INTERFACE:$<INSTALL_PREFIX>/${CMAKE_INSTALL_INCLUDEDIR}>
)

# Installation

## DataFrame library & public headers
install(
    TARGETS DataFrame
    EXPORT DataFrameExport
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT lib
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT lib
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT bin
)

install(
    DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/DataFrame
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    COMPONENT dev
)

## pkgconfig file
if(UNIX AND NOT APPLE)
    set(DATAFRAME_PKG_EXTRA_LIBS " -lpthread -lrt")
endif()

configure_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/cmake/DataFrame.pc.in
    DataFrame.pc
    @ONLY
)

install(
    FILES ${CMAKE_CURRENT_BINARY_DIR}/DataFrame.pc
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig
)

## CMake config files
configure_package_config_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/cmake/DataFrameConfig.cmake.in
    DataFrameConfig.cmake
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/DataFrame
)

write_basic_package_version_file(
    DataFrameConfigVersion.cmake
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY AnyNewerVersion
)

export(
    EXPORT DataFrameExport
    NAMESPACE DataFrame::
    FILE DataFrameTargets.cmake
)

install(
    EXPORT DataFrameExport
    NAMESPACE DataFrame::
    FILE DataFrameTargets.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/DataFrame
)

install(
    FILES ${CMAKE_CURRENT_BINARY_DIR}/DataFrameConfig.cmake
          ${CMAKE_CURRENT_BINARY_DIR}/DataFrameConfigVersion.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/DataFrame
)

export(PACKAGE DataFrame)

# Tests
if(HMDF_TESTING)
    add_subdirectory(test)
    file(COPY ${DATA_FILES} DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/test)
endif()

# Examples
if(HMDF_EXAMPLES)
    add_subdirectory(examples)
endif()

# Benchmarks
if(HMDF_BENCHMARKS)
    add_subdirectory(benchmarks)
endif()
