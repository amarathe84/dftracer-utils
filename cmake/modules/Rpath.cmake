# Use RPATH on OS X
if(APPLE)
  set(CMAKE_MACOSX_RPATH ON)
endif()

# Use (i.e. don't skip) RPATH for build
set(CMAKE_SKIP_BUILD_RPATH FALSE)

# Use same RPATH for build and install
set(CMAKE_BUILD_WITH_INSTALL_RPATH FALSE)

# Add the automatically determined parts of the RPATH which point to directories
# outside the build tree to the install RPATH
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

function(normalize_rpath rpaths output)
  set(result_str "")
  if(APPLE)
    # in APPLE, we use ;
    foreach(rpath IN LISTS rpaths)
      string(REPLACE "$ORIGIN" "@loader_path" rpath "${rpath}")
      set(result_str "${result_str};${rpath}")
    endforeach()
  else()
    foreach(rpath IN LISTS rpaths)
      string(REPLACE "$ORIGIN" "@loader_path" rpath "${rpath}")
      set(result_str "${result_str}:${rpath}")
    endforeach()
  endif()
  set(output
      "${result_str}"
      PARENT_SCOPE)
endfunction()

function(target_add_rpath target_name)
  set(rpath_list "")
  set(additional_paths ${ARGN})
  if(APPLE)
    list(APPEND rpath_list "@loader_path" "@loader_path/lib")
    foreach(path IN LISTS additional_paths)
      list(APPEND rpath_list "${path}")
    endforeach()
    normalize_rpath(rpath_list rpath_list)
  else()
    list(APPEND rpath_list "$ORIGIN" "$ORIGIN/lib" "$ORIGIN/../lib"
         "$ORIGIN/../lib64")
    foreach(path IN LISTS additional_paths)
      list(APPEND rpath_list "${path}")
    endforeach()
  endif()
  message(STATUS "RPATH for target ${target_name}: ${rpath_list}")
  set_target_properties(
    ${target_name}
    PROPERTIES INSTALL_RPATH "${rpath_list}"
               BUILD_RPATH "${rpath_list}"
               BUILD_WITH_INSTALL_RPATH TRUE
               INSTALL_RPATH_USE_LINK_PATH TRUE)
endfunction()
