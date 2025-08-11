set(CPM_USE_LOCAL_PACKAGES ON)
set(CPM_SOURCE_CACHE "${CMAKE_SOURCE_DIR}/.cpmsource")

function(need_spdlog)
  # Force CPM build for spdlog to avoid system package compatibility issues
  if(NOT spdlog_ADDED)
    CPMAddPackage(
      NAME spdlog
      GITHUB_REPOSITORY gabime/spdlog
      VERSION 1.12.0
      OPTIONS
        "SPDLOG_COMPILED_LIB ON"
        "SPDLOG_BUILD_SHARED ON"
        "SPDLOG_INSTALL ON"
      FORCE YES
    )
  endif()
endfunction()

function(need_argparse)
  if(NOT argparse_ADDED)
    CPMAddPackage(
      NAME argparse
      GITHUB_REPOSITORY p-ranav/argparse
      VERSION 3.2
      OPTIONS
        "ARGPARSE_BUILD_TESTS OFF"
        "ARGPARSE_BUILD_SAMPLES OFF"
      FORCE YES
    )
  endif()
endfunction()

function(need_ghc_filesystem)
  if(NOT ghc_filesystem_ADDED)
    CPMAddPackage(
      NAME ghc_filesystem
      GITHUB_REPOSITORY gulrak/filesystem
      VERSION 1.5.14
      OPTIONS
        "GHC_FILESYSTEM_WITH_INSTALL ON"
      FORCE YES
    )
  endif()
endfunction()

function(need_sqlite3)
  find_package(SQLite3 3.30 QUIET)
  
  if(SQLite3_FOUND)
    message(STATUS "Found system SQLite3: ${SQLite3_LIBRARIES}")
    
    # Create alias for system SQLite3 if it doesn't exist
    if(NOT TARGET SQLite::SQLite3)
      # Create imported target for system SQLite3
      add_library(SQLite::SQLite3 UNKNOWN IMPORTED)
      set_target_properties(SQLite::SQLite3 PROPERTIES
        IMPORTED_LOCATION "${SQLite3_LIBRARIES}"
        INTERFACE_INCLUDE_DIRECTORIES "${SQLite3_INCLUDE_DIRS}"
      )
    endif()
    
    if(NOT TARGET SQLite::SQLite3_static)
      add_library(SQLite::SQLite3_static ALIAS SQLite::SQLite3)
    endif()
    
    # Set variables in parent scope so they persist outside the function
    set(SQLite3_FOUND ${SQLite3_FOUND} PARENT_SCOPE)
    set(SQLite3_LIBRARIES ${SQLite3_LIBRARIES} PARENT_SCOPE)
    set(SQLite3_INCLUDE_DIRS ${SQLite3_INCLUDE_DIRS} PARENT_SCOPE)
    set(SQLite3_CPM FALSE PARENT_SCOPE)
  else()
    # Build with CPM
    if(NOT SQLite3_ADDED)
      CPMAddPackage(
        NAME SQLite3
        URL https://www.sqlite.org/2024/sqlite-amalgamation-3460100.zip
        VERSION 3.46.1
        DOWNLOAD_ONLY YES
      )
    endif()

    if(SQLite3_ADDED)
      message(STATUS "Built SQLite3 with CPM")
      
      # Create sqlite3 library from amalgamation
      add_library(sqlite3 SHARED
        ${SQLite3_SOURCE_DIR}/sqlite3.c
      )

      add_library(sqlite3_static STATIC
        ${SQLite3_SOURCE_DIR}/sqlite3.c
      )

      target_include_directories(sqlite3 PUBLIC 
        $<BUILD_INTERFACE:${SQLite3_SOURCE_DIR}>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
      )

      target_include_directories(sqlite3_static PUBLIC
        $<BUILD_INTERFACE:${SQLite3_SOURCE_DIR}>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
      )
      
      # Enable common SQLite features
      target_compile_definitions(sqlite3 PUBLIC
        SQLITE_ENABLE_FTS5
        SQLITE_ENABLE_JSON1
        SQLITE_ENABLE_RTREE
        SQLITE_THREADSAFE=1
      )

      target_compile_definitions(sqlite3_static PUBLIC
        SQLITE_ENABLE_FTS5
        SQLITE_ENABLE_JSON1
        SQLITE_ENABLE_RTREE
        SQLITE_THREADSAFE=1
      )

      if(NOT WIN32)
        target_link_libraries(sqlite3 PRIVATE pthread dl m)
        target_link_libraries(sqlite3_static PRIVATE pthread dl m)
      endif()
      
      # Create alias for compatibility
      add_library(SQLite::SQLite3 ALIAS sqlite3)
      add_library(SQLite::SQLite3_static ALIAS sqlite3_static)
      
      # Make sqlite3 installable
      install(TARGETS sqlite3 sqlite3_static
        EXPORT sqlite3Targets
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
      )
      
      # Install sqlite3 header
      install(FILES ${SQLite3_SOURCE_DIR}/sqlite3.h
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
      )

      set(SQLite3_CPM TRUE PARENT_SCOPE)
    endif()
  endif()
endfunction()

# Function to link SQLite3 to a target
# Parameters:
#   TARGET_NAME - name of the target to link SQLite3 to
function(link_sqlite3 TARGET_NAME LIBRARY_TYPE)
  # Validate parameters
  if(NOT TARGET_NAME)
    message(FATAL_ERROR "link_sqlite3: TARGET_NAME is required")
  endif()
  
  if(NOT TARGET ${TARGET_NAME})
    message(FATAL_ERROR "link_sqlite3: Target '${TARGET_NAME}' does not exist")
  endif()
  
  # Check if any SQLite3 variant is available
  set(SQLITE3_AVAILABLE FALSE)
  
  # Check for CPM-built SQLite3
  if(TARGET sqlite3 OR TARGET sqlite3_static)
    set(SQLITE3_AVAILABLE TRUE)
  endif()
  
  # Check for system SQLite3
  if(TARGET SQLite::SQLite3)
    set(SQLITE3_AVAILABLE TRUE)
  endif()
  
  if(NOT SQLITE3_AVAILABLE)
    message(FATAL_ERROR "link_sqlite3: No SQLite3 found! Call need_sqlite3() first or ensure system SQLite3 is available.")
  endif()
  
  # Link appropriate SQLite3 variant
  if(LIBRARY_TYPE STREQUAL "STATIC")
    # For static libraries, prefer static SQLite3
    if(TARGET sqlite3_static)
      target_link_libraries(${TARGET_NAME} PRIVATE SQLite::SQLite3_static)
      message(STATUS "Linked ${TARGET_NAME} to CPM-built SQLite::SQLite3_static")
    elseif(TARGET SQLite::SQLite3_static)
      target_link_libraries(${TARGET_NAME} PRIVATE SQLite::SQLite3_static)
      message(STATUS "Linked ${TARGET_NAME} to SQLite::SQLite3_static")
    elseif(TARGET sqlite3)
      target_link_libraries(${TARGET_NAME} PRIVATE SQLite::SQLite3)
      message(STATUS "Linked ${TARGET_NAME} to CPM-built SQLite::SQLite3")
    elseif(TARGET SQLite::SQLite3)
      target_link_libraries(${TARGET_NAME} PRIVATE SQLite::SQLite3)
      message(STATUS "Linked ${TARGET_NAME} to SQLite::SQLite3")
    endif()
  else()
    # For shared libraries, prefer shared SQLite3
    if(TARGET sqlite3)
      target_link_libraries(${TARGET_NAME} PRIVATE SQLite::SQLite3)
      message(STATUS "Linked ${TARGET_NAME} to CPM-built SQLite::SQLite3")
    elseif(TARGET SQLite::SQLite3)
      target_link_libraries(${TARGET_NAME} PRIVATE SQLite::SQLite3)
      message(STATUS "Linked ${TARGET_NAME} to SQLite::SQLite3")
    elseif(TARGET sqlite3_static)
      target_link_libraries(${TARGET_NAME} PRIVATE SQLite::SQLite3_static)
      message(STATUS "Linked ${TARGET_NAME} to CPM-built SQLite::SQLite3_static")
    elseif(TARGET SQLite::SQLite3_static)
      target_link_libraries(${TARGET_NAME} PRIVATE SQLite::SQLite3_static)
      message(STATUS "Linked ${TARGET_NAME} to SQLite::SQLite3_static")
    endif()
  endif()
endfunction()

function(need_zlib)
  find_package(ZLIB 1.2 QUIET)

  if(ZLIB_FOUND)
    message(STATUS "Found system ZLIB: ${ZLIB_LIBRARIES}")
    
    # Set variables in parent scope so they persist outside the function
    set(ZLIB_FOUND ${ZLIB_FOUND} PARENT_SCOPE)
    set(ZLIB_LIBRARIES ${ZLIB_LIBRARIES} PARENT_SCOPE)
    set(ZLIB_INCLUDE_DIRS ${ZLIB_INCLUDE_DIRS} PARENT_SCOPE)
    set(ZLIB_CPM FALSE PARENT_SCOPE)
  else()
    set(ZLIB_CPM FALSE PARENT_SCOPE)
    # Build with CPM
    CPMAddPackage(
      NAME ZLIB
      GITHUB_REPOSITORY madler/zlib
      VERSION 1.3.1
      OPTIONS
        "ZLIB_BUILD_STATIC ON"
        "ZLIB_BUILD_SHARED ON"
        "ZLIB_INSTALL OFF"
        "ZLIB_BUILD_EXAMPLES OFF"
    )
    
    if(ZLIB_ADDED)
      message(STATUS "Built ZLIB with CPM")
      set(ZLIB_CPM TRUE PARENT_SCOPE) 

      # Make sure the source and binary directories are available in parent scope
      set(ZLIB_SOURCE_DIR ${ZLIB_SOURCE_DIR} PARENT_SCOPE)
      set(ZLIB_BINARY_DIR ${ZLIB_BINARY_DIR} PARENT_SCOPE)

      # Completely override zlib's cmake_install.cmake after configuration
      # This works because zlib regenerates its install file, but we'll override it post-generation
      add_custom_target(override_zlib_install
        ALL
        COMMAND ${CMAKE_COMMAND} -E echo "# Installation disabled for zlib to prevent duplicates" > "${ZLIB_BINARY_DIR}/cmake_install.cmake"
        COMMAND ${CMAKE_COMMAND} -E echo "message(STATUS \"Skipping zlib installation - handled manually\")" >> "${ZLIB_BINARY_DIR}/cmake_install.cmake"
        DEPENDS zlib zlibstatic
        COMMENT "Preventing zlib from installing files automatically"
        VERBATIM
      )
      
      # Also prevent any subdirectory installs
      install(CODE "
        message(STATUS \"Preventing any residual zlib installation\")
        # This prevents any zlib install commands from executing
      ")

      # Add zlib targets if they're CPM-built (our own targets)
      # We manually install zlib because ZLIB_INSTALL is problematic
      if(TARGET zlib)
          # Manual installation of zlib to the correct location
          install(TARGETS zlib
              ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
              LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
              RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
          )
      endif()
      if(TARGET zlibstatic)
          install(TARGETS zlibstatic
              ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
              LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
              RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
          )
      endif()

      # Install zlib headers manually
      if(ZLIB_SOURCE_DIR AND ZLIB_BINARY_DIR)
          # Check if the header files actually exist before trying to install them
          if(EXISTS "${ZLIB_SOURCE_DIR}/zlib.h")
              install(FILES "${ZLIB_SOURCE_DIR}/zlib.h"
                  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
              )
          endif()
          
          if(EXISTS "${ZLIB_BINARY_DIR}/zconf.h")
              install(FILES "${ZLIB_BINARY_DIR}/zconf.h"
                  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
              )
          endif()
      else()
          message(WARNING "Using ZLIB with CPM but source/binary directories not found. Skipping zlib header installation.")
      endif()

      # Create simple aliases - these won't be exportable but that's OK
      # The consumer will need to find their own zlib
      if(TARGET zlib AND NOT TARGET ZLIB::ZLIB)
        add_library(ZLIB::ZLIB ALIAS zlib)
      endif()

      if(TARGET zlibstatic AND NOT TARGET ZLIB::ZLIBSTATIC)
        add_library(ZLIB::ZLIBSTATIC ALIAS zlibstatic)
      endif()
    endif()
  endif()
endfunction()

# Function to link zlib to a target
# Parameters:
#   TARGET_NAME - name of the target to link zlib to
#   LIBRARY_TYPE - "STATIC" or "SHARED" to choose appropriate zlib variant
function(link_zlib TARGET_NAME LIBRARY_TYPE)
  # Validate parameters
  if(NOT TARGET_NAME)
    message(FATAL_ERROR "link_zlib: TARGET_NAME is required")
  endif()
  
  if(NOT LIBRARY_TYPE MATCHES "^(STATIC|SHARED)$")
    message(FATAL_ERROR "link_zlib: LIBRARY_TYPE must be either STATIC or SHARED")
  endif()
  
  if(NOT TARGET ${TARGET_NAME})
    message(FATAL_ERROR "link_zlib: Target '${TARGET_NAME}' does not exist")
  endif()
  
  # Check if any zlib variant is available
  set(ZLIB_AVAILABLE FALSE)
  if(TARGET zlibstatic OR TARGET zlib OR ZLIB_FOUND)
    set(ZLIB_AVAILABLE TRUE)
  endif()
  
  if(NOT ZLIB_AVAILABLE)
    message(FATAL_ERROR "link_zlib: No zlib found! Call need_zlib() first or ensure system zlib is available.")
  endif()
  
  # Link appropriate zlib variant
  if(LIBRARY_TYPE STREQUAL "STATIC")
    # For static libraries, prefer static zlib if available
    if(TARGET zlibstatic)
      # CPM-built zlib static - use generator expression to avoid export issues
      target_include_directories(${TARGET_NAME} PRIVATE ${ZLIB_SOURCE_DIR} ${ZLIB_BINARY_DIR})
      target_link_libraries(${TARGET_NAME} PRIVATE $<TARGET_FILE:zlibstatic>)
      message(STATUS "Linked ${TARGET_NAME} to CPM-built zlibstatic")
    elseif(TARGET zlib)
      # CPM-built zlib shared - use generator expression to avoid export issues
      target_include_directories(${TARGET_NAME} PRIVATE ${ZLIB_SOURCE_DIR} ${ZLIB_BINARY_DIR})
      target_link_libraries(${TARGET_NAME} PRIVATE $<TARGET_FILE:zlib>)
      message(STATUS "Linked ${TARGET_NAME} to CPM-built zlib (shared)")
    elseif(ZLIB_FOUND)
      # System zlib - use normal linking
      target_link_libraries(${TARGET_NAME} PRIVATE ZLIB::ZLIB)
      message(STATUS "Linked ${TARGET_NAME} to system ZLIB::ZLIB")
    endif()
  else() # SHARED
    # For shared libraries, prefer shared zlib if available
    if(TARGET zlib)
      # CPM-built zlib shared - use generator expression to avoid export issues
      target_include_directories(${TARGET_NAME} PRIVATE ${ZLIB_SOURCE_DIR} ${ZLIB_BINARY_DIR})
      target_link_libraries(${TARGET_NAME} PRIVATE $<TARGET_FILE:zlib>)
      message(STATUS "Linked ${TARGET_NAME} to CPM-built zlib (shared)")
    elseif(TARGET zlibstatic)
      # CPM-built zlib static - use generator expression to avoid export issues
      target_include_directories(${TARGET_NAME} PRIVATE ${ZLIB_SOURCE_DIR} ${ZLIB_BINARY_DIR})
      target_link_libraries(${TARGET_NAME} PRIVATE $<TARGET_FILE:zlibstatic>)
      message(STATUS "Linked ${TARGET_NAME} to CPM-built zlibstatic")
    elseif(ZLIB_FOUND)
      # System zlib - use normal linking
      target_link_libraries(${TARGET_NAME} PRIVATE ZLIB::ZLIB)
      message(STATUS "Linked ${TARGET_NAME} to system ZLIB::ZLIB")
    endif()
  endif()
endfunction()

function(need_picosha2)
  if(NOT PicoSHA2_ADDED)
    CPMAddPackage(
      NAME PicoSHA2
      GITHUB_REPOSITORY okdshin/PicoSHA2
      VERSION 1.0.1
      GIT_TAG "v1.0.1"
      DOWNLOAD_ONLY YES
    )
    
    if(PicoSHA2_ADDED)
      add_library(picosha2 INTERFACE)
      target_include_directories(picosha2 INTERFACE
          $<BUILD_INTERFACE:${PicoSHA2_SOURCE_DIR}>
          $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
      )
      install(FILES ${PicoSHA2_SOURCE_DIR}/picosha2.h DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
      message(STATUS "Added picosha2 header-only library")
    endif()
  endif()
endfunction()
