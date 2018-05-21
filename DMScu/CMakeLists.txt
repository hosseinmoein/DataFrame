message(STATUS "-- Configuring DMscu module")

set(SOURCE
  src/DMScu_FileBase.cc
  src/DMScu_MMapBase.cc
  src/DMScu_MMapFile.cc
  )
set(HEADERS
  include/DMScu_Exception.h
  include/DMScu_FileBase.h
  include/DMScu_FileDef.h
  include/DMScu_FixedSizeString.h
  include/DMScu_MMapBase.h
  include/DMScu_MMapFile.h
  )

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
install(FILES ${HEADERS} DESTINATION include)

message(STATUS "-- Configuring DMscu module - done")
