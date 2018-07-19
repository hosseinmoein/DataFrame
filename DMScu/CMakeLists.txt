project (DMScu VERSION 1.0.0 LANGUAGES CXX)

set(LIBRARY_TARGET_NAME ${PROJECT_NAME})

# List of source files.
set(${LIBRARY_TARGET_NAME}_SRC
  src/DMScu_FileBase.cc
  src/DMScu_MMapBase.cc
  src/DMScu_MMapFile.cc
  )

# List of header files.
set(${LIBRARY_TARGET_NAME}_HDR
  include/DMScu_Exception.h
  include/DMScu_FileBase.h
  include/DMScu_FileDef.h
  include/DMScu_FixedSizeString.h
  include/DMScu_MMapBase.h
  include/DMScu_MMapFile.h
)

# Build the DMScu library
add_library(${LIBRARY_TARGET_NAME} SHARED ${${LIBRARY_TARGET_NAME}_SRC})

# Set two minimum target properties for the library.
# See https://cmake.org/cmake/help/latest/command/set_target_properties.html
# Properties are: 1) Library version number 2) list of corresponding public headers
set_target_properties(${LIBRARY_TARGET_NAME} PROPERTIES VERSION       ${${PROJECT_NAME}_VERSION}
                                                        PUBLIC_HEADER "${${LIBRARY_TARGET_NAME}_HDR}")

# Specify include directories for both compilation and installation process.
# The $<INSTALL_PREFIX> generator expression is useful to ensure to create
# relocatable configuration files, see https://cmake.org/cmake/help/latest/manual/cmake-packages.7.html#creating-relocatable-packages
target_include_directories(${LIBRARY_TARGET_NAME} PUBLIC "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
							 "$<INSTALL_INTERFACE:$<INSTALL_PREFIX>/${CMAKE_INSTALL_INCLUDEDIR}>")

# Specify installation targets, typology and destination folders.
install(TARGETS ${LIBRARY_TARGET_NAME}
        EXPORT  ${PROJECT_NAME}
        LIBRARY       DESTINATION "${CMAKE_INSTALL_LIBDIR}"                            COMPONENT shlib
        ARCHIVE       DESTINATION "${CMAKE_INSTALL_LIBDIR}"                            COMPONENT lib
        RUNTIME       DESTINATION "${CMAKE_INSTALL_BINDIR}"                            COMPONENT bin
        PUBLIC_HEADER DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/${LIBRARY_TARGET_NAME}" COMPONENT dev)

# Create and install CMake configuration files for your project that are
# necessary to for other projects to call find_package().
#
# Note that it is extremely important to use exactly the project name while installing configuration
# files (you can use PROJECT_NAME variable to avoid any possible error). This is required to allow
# find_package() to properly look for the installed library in system path, in particular in Windows
# when the installation is performed in the default path.
#
# install_basic_package_files() comes with many input parameters to customize the configuration
# files. The parameters used in the following call provide basic versions of CMake configuration
# files.  See install_basic_package_files() documentation found in ./cmake folder.
#
# Note that if your library depends from other libraries, you are probably
# required to used the install_basic_package_files() DEPENDENCIES option.
include(InstallBasicPackageFiles)
install_basic_package_files(${PROJECT_NAME}
                            VERSION ${${PROJECT_NAME}_VERSION}
                            COMPATIBILITY AnyNewerVersion
                            EXPORT ${PROJECT_NAME}
                            VARS_PREFIX ${PROJECT_NAME}
                            NO_CHECK_REQUIRED_COMPONENTS_MACRO)

# Build the test binary
add_executable(filebase_tester src/filebase_tester.cc)
add_executable(fixsizestr_tester src/fixsizestr_tester.cc)
add_executable(mmfile_tester src/mmfile_tester.cc)
target_link_libraries(filebase_tester DMScu)
target_link_libraries(fixsizestr_tester DMScu)
target_link_libraries(mmfile_tester DMScu)

# Enable ctest, testing so we can see if unit tests pass or fail in CI
enable_testing()
add_test(NAME filebase_tester
         COMMAND filebase_tester
         WORKING_DIRECTORY $<TARGET_FILE_DIR:filebase_tester>)
add_test(NAME fixsizestr_tester
         COMMAND fixsizestr_tester
         WORKING_DIRECTORY $<TARGET_FILE_DIR:fixsizestr_tester>)
add_test(NAME mmfile_tester
         COMMAND mmfile_tester
         WORKING_DIRECTORY $<TARGET_FILE_DIR:mmfile_tester>)

message("-- Copying files for testing")
file(COPY src/DMScu_FileBase.cc DESTINATION ${CMAKE_BINARY_DIR}/bin)
message("-- Copying files for testing - done")
