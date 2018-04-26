cmake_minimum_required (VERSION 3.0)
project (DataFrame)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON) # Produce compile_commands.json

#Build library
set(SOURCE
  DMScu/src/DMScu_FileBase.cc
  DMScu/src/DMScu_MMapBase.cc
  DMScu/src/DMScu_MMapFile.cc
  DMScu/src/filebase_tester.cc
  DMScu/src/fixsizestr_tester.cc
  DMScu/src/mmfile_tester.cc )
set(HEADERS
  DMScu/include/DMScu_Exception.h
  DMScu/include/DMScu_FileBase.h
  DMScu/include/DMScu_FileDef.h
  DMScu/include/DMScu_FixedSizeString.h
  DMScu/include/DMScu_MMapBase.h
  DMScu/include/DMScu_MMapFile.h
  )

add_library(dataframe SHARED ${SOURCE})
install(TARGETS dataframe DESTINATION lib) # Move the lib file
install(FILES ${HEADERS} DESTINATION include) # Move the header files

# Build test
# add_executable(datasci_tester src/datasci_tester.cc src/BaseContainer.cc)
