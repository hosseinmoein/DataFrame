cmake_minimum_required (VERSION 3.2)
project (Dataframe VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON) # Produce compile_commands.json

message("-- Copying files for testing")
file(GLOB TEST_DATA "test/*.csv") # Get all txt files into a variable
if (UNIX)
  file(COPY ${TEST_DATA} DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
endif(UNIX)
if (MSVC)
  file(COPY ${TEST_DATA} DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/Debug)
  file(COPY ${TEST_DATA} DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/Release)
endif(MSVC)
message("-- Copying files for testing - done")

add_subdirectory(DMScu) # Build DMscu module first

set(SOURCE src/BaseContainer.cc)

# Build the Dataframe library
add_definitions(-D DMS_INCLUDE_SOURCE) # Need this for some of the .tcc files to be compiled
add_library(Dataframe SHARED ${SOURCE})
add_library(Dataframe::Dataframe ALIAS Dataframe)
target_compile_features(Dataframe PUBLIC cxx_std_17)
target_link_libraries(Dataframe DMScu) # Link DMscu to Dataframe
set_target_properties(Dataframe PROPERTIES
  ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib
  LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib
  RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
  )
# Install the library
include(GNUInstallDirs)
set(config_install_dir "${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}")
set(generated_dir "${CMAKE_CURRENT_BINARY_DIR}/generated")
# Configuration
set(version_config "${generated_dir}/${PROJECT_NAME}ConfigVersion.cmake")
set(project_config "${generated_dir}/${PROJECT_NAME}Config.cmake")
set(TARGETS_EXPORT_NAME "${PROJECT_NAME}Targets")
set(namespace "${PROJECT_NAME}::")
# Include module with fuction 'write_basic_package_version_file'
include(CMakePackageConfigHelpers)
# Configure DataframeConfigVersion.cmake
write_basic_package_version_file(
  "${version_config}" COMPATIBILITY SameMajorVersion
  )
# Configure Project DataframeConfig.cmake
configure_package_config_file(
  "cmake/Dataframe-config.cmake.in"
  "${project_config}"
  INSTALL_DESTINATION "${config_install_dir}"
  )
install(TARGETS Dataframe # Configure targets to be exported and install the library
  EXPORT Dataframe-targets
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
  INCLUDES DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
  )
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR} FILES_MATCHING PATTERN "*.h") # Move the header files
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR} FILES_MATCHING PATTERN "*.tcc") # Move the tcc files
install(
  FILES "${project_config}" "${version_config}" # Copy the library config files so other projects using cmake can find DMScu
  DESTINATION "${config_install_dir}"
  )
install(EXPORT Dataframe-targets # Export the targets created above so other project can find Dataframe
  FILE Dataframe-targets.cmake
  NAMESPACE Dataframe::
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/Dataframe
  )

# Build the test binary
add_executable(datasci_tester src/datasci_tester.cc)
target_link_libraries(datasci_tester Dataframe) # Link the Dataframe library to the test binary
if (UNIX)
  # Find pthreads library
  set(THREADS_PREFER_PTHREAD_FLAG ON)
  find_package(Threads REQUIRED)
  target_link_libraries(datasci_tester Threads::Threads)
endif (UNIX)

# Enable ctest, testing so we can see if unit tests pass or fail in CI
enable_testing()
add_test(Datasci_Tester datasci_tester)
