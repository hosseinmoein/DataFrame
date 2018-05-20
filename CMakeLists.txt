cmake_minimum_required (VERSION 3.0.2)
project (DataFrame)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON) # Produce compile_commands.json

add_subdirectory(DMScu) # Build DMscu module first

set(SOURCE
  src/BaseContainer.cc
  )
set(HEADERS
  include/BaseContainer.h
  include/BaseContainer.tcc
  include/DFVisitors.h
  include/DataFrame.h
  include/DataFrame.tcc
  include/DataFrame_get.tcc
  include/DataFrame_misc.tcc
  include/DataFrame_opt.tcc
  include/DataFrame_read.tcc
  include/DataFrame_set.tcc
  )

# Build the Dataframe library
add_library(Dataframe SHARED ${SOURCE})
target_link_libraries(Dataframe DMScu) # Link DMscu to Dataframe

# Build the test binary
add_executable(datasci_tester src/datasci_tester.cc)
target_link_libraries(datasci_tester Dataframe) # Link the Dataframe library to the test binary

# Install the library
install(TARGETS Dataframe DESTINATION lib) # Move the lib file
install(FILES ${HEADERS} DESTINATION include) # Move the header files
