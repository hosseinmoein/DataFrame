project (DMScu VERSION 1.0.0 LANGUAGES CXX)

message(STATUS "-- Configuring DMscu module")
set(SOURCE
  src/DMScu_FileBase.cc
  src/DMScu_MMapBase.cc
  src/DMScu_MMapFile.cc
  )

message("-- Copying files for testing")
if (UNIX)
  file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/src/DMScu_FileBase.cc DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
endif(UNIX)
message("-- Copying files for testing - done")

# Build the DMScu library
add_library(DMScu SHARED ${SOURCE})
add_library(DMScu::DMScu ALIAS DMScu)
target_compile_features(DMScu PUBLIC cxx_std_17)
set_target_properties(DMScu PROPERTIES
  ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib
  LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib
  RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
  )

# Build the test binary
add_executable(filebase_tester src/filebase_tester.cc)
add_executable(fixsizestr_tester src/fixsizestr_tester.cc)
add_executable(mmfile_tester src/mmfile_tester.cc)

# Link the Dataframe library to the test binary
target_link_libraries(filebase_tester DMScu)
target_link_libraries(fixsizestr_tester DMScu)
target_link_libraries(mmfile_tester DMScu)

# Install the library/module
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
# Configure 'DMScuConfigVersion.cmake'
write_basic_package_version_file(
  "${version_config}" COMPATIBILITY SameMajorVersion
  )
# Configure 'DMScuConfig.cmake'
configure_package_config_file(
  "cmake/DMScu-config.cmake.in"
  "${project_config}"
  INSTALL_DESTINATION "${config_install_dir}"
  )
install(TARGETS DMScu # Configure targets to be exported and install the library
  EXPORT DMScu-targets
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
  INCLUDES DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
  )
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR} FILES_MATCHING PATTERN "*.h")
install(
  FILES "${project_config}" "${version_config}" # Copy the library config files so other projects using cmake can find DMScu
  DESTINATION "${config_install_dir}"
  )
install(EXPORT DMScu-targets # Export the targets created above so other project can find DMScu
  FILE DMScu-targets.cmake
  NAMESPACE DMScu::
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/DMScu
  )
message(STATUS "-- Configuring DMscu module - done")

# Enable ctest, testing so we can see if unit tests pass or fail in CI
enable_testing()
add_test(Filebase_Tester filebase_tester)
add_test(Fixsizestr_Tester fixsizestr_tester)
add_test(Mmfile_Tester mmfile_tester)
