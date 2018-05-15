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

add_executable(filebase_tester src/filebase_tester.cc)
add_executable(fixsizestr_tester src/fixsizestr_tester.cc)
add_executable(mmfile_tester.cc src/mmfile_tester.cc)

add_library(DMSCu SHARED ${SOURCE})
install(TARGETS DMSCu DESTINATION lib)
install(FILES ${HEADERS} DESTINATION include)
