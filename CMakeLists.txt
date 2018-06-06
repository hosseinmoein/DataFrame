cmake_minimum_required (VERSION 3.2)
project (DataFrame)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON) # Produce compile_commands.json
set(CMAKE_CXX_STANDARD 14) # C++14
set(CMAKE_CXX_STANDARD_REQUIRED ON)

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

set(SOURCE
  src/BaseContainer.cc
  )

# Build the Dataframe library
add_definitions(-D DMS_INCLUDE_SOURCE) # Need this for some of the .tcc files to be compiled
add_library(Dataframe SHARED ${SOURCE})
target_link_libraries(Dataframe DMScu) # Link DMscu to Dataframe
target_include_directories(Dataframe INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/DMScu/include)
target_include_directories(Dataframe INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/include)

# Build the test binary
add_executable(datasci_tester src/datasci_tester.cc)
target_link_libraries(datasci_tester Dataframe) # Link the Dataframe library to the test binary

if (UNIX)
  # Find pthreads library
  set(THREADS_PREFER_PTHREAD_FLAG ON)
  find_package(Threads REQUIRED)
  target_link_libraries(datasci_tester Threads::Threads)
endif (UNIX)

# Install the library
install(TARGETS Dataframe DESTINATION lib) # Move the lib file
install(DIRECTORY include/ DESTINATION include FILES_MATCHING PATTERN "*.h") # Move the header files
install(DIRECTORY include/ DESTINATION include FILES_MATCHING PATTERN "*.tcc") # Move the tcc files

# Enable ctest, testing so we can see if unit tests pass or fail in CI
enable_testing()
add_test(Datasci_Tester datasci_tester)
