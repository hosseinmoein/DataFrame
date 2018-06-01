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

# Build the test binary
add_executable(filebase_tester src/filebase_tester.cc)
add_executable(fixsizestr_tester src/fixsizestr_tester.cc)
add_executable(mmfile_tester src/mmfile_tester.cc)

# Link the Dataframe library to the test binary
target_link_libraries(filebase_tester DMScu)
target_link_libraries(fixsizestr_tester DMScu)
target_link_libraries(mmfile_tester DMScu)

# Install the library/module
install(TARGETS DMScu DESTINATION lib)
install(DIRECTORY include/ DESTINATION include FILES_MATCHING PATTERN "*.h")
message(STATUS "-- Configuring DMscu module - done")

# Enable ctest, testing so we can see if unit tests pass or fail in CI
enable_testing()
add_test(Filebase_Tester filebase_tester)
add_test(Fixsizestr_Tester fixsizestr_tester)
# add_test(Mmfile_Tester mmfile_tester)  # Disabling this for now, this is never exiting
