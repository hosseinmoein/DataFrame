function(dataframe_target_data_files)
    cmake_parse_arguments(ARG "" "TARGET" "DATA_FILES" ${ARGN})

    foreach(_data_file IN LISTS ARG_DATA_FILES)
        get_filename_component(_data_file_name ${_data_file} NAME)
        set(_data_file_target copy_${_data_file_name})
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
        add_dependencies(${ARG_TARGET} ${_data_file_target})
    endforeach()
endfunction()
