#=======================================================================
# HmdfTargetDataFiles
# -------------------
#
# Function to associate required data files of DataFrame tests and samples
# targets, and copy them to the appropriate location in the build tree
# when needed.
#
#    hmdf_target_data_files(
#        <target>
#        [DATA_FILES <files_path_list>]
#    )
#
#=======================================================================
function(hmdf_target_data_files targetName)
    set(prefix ARG)
    set(noValues "")
    set(singleValues "")
    set(multiValues DATA_FILES)

    cmake_parse_arguments(
        ${prefix}
        "${noValues}"
        "${singleValues}"
        "${multiValues}"
        ${ARGN}
    )

    foreach(_data_file IN LISTS ARG_DATA_FILES)
        get_filename_component(_data_file_name ${_data_file} NAME)
        set(_data_file_target hmdf_copy_${_data_file_name})
        if(NOT TARGET ${_data_file_target})
            set(_data_file_output
                ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/${CMAKE_CFG_INTDIR}/data/${_data_file_name})
            add_custom_target(${_data_file_target}
                DEPENDS ${_data_file_output}
            )
            get_filename_component(_data_file_abs_path ${_data_file} REALPATH)
            add_custom_command(
                OUTPUT ${_data_file_output}
                COMMAND ${CMAKE_COMMAND} -E copy_if_different
                        ${_data_file_abs_path} ${_data_file_output}
                DEPENDS ${_data_file_abs_path}
            )
        endif()
        add_dependencies(${targetName} ${_data_file_target})

        # Also add a convenient custom target to copy all files if needed
        if(TARGET hmdf_copy_all_data_files)
            add_dependencies(hmdf_copy_all_data_files ${_data_file_target})
        else()
            add_custom_target(hmdf_copy_all_data_files DEPENDS ${_data_file_target})
        endif()
    endforeach()
endfunction()
