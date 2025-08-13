cmake_minimum_required(VERSION 3.15...3.26)

project(dft_utils C CXX)

if (NOT SKBUILD)
  message(WARNING "\
  This CMake file is meant to be executed using 'scikit-build'. Running
  it directly will almost certainly not produce the desired result. If
  you are a user trying to install this package, please use the command
  below, which will install all necessary build dependencies, compile
  the package in an isolated environment, and then install it.
  =====================================================================
   $ pip install .
  =====================================================================
  If you are a software developer, and this is your own package, then
  it is usually much more efficient to install the build dependencies
  in your environment once and use the following command that avoids
  a costly creation of a new virtual environment at every compilation:
  =====================================================================
   $ pip install nanobind scikit-build-core[pyproject]
   $ pip install --no-build-isolation -ve .
  =====================================================================
  You may optionally add -Ceditable.rebuild=true to auto-rebuild when
  the package is imported. Otherwise, you need to re-run the above
  after editing C++ files.")
endif()

# Set C++ standard
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Enable optimization
set(CMAKE_BUILD_TYPE Debug CACHE STRING "Build type" FORCE)
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0" CACHE STRING "Debug flags" FORCE)
set(CMAKE_C_FLAGS_DEBUG "-g -O0" CACHE STRING "Debug flags" FORCE)

# macOS SDK compatibility for GCC - use older deployment target
if (APPLE AND CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    set(CMAKE_OSX_DEPLOYMENT_TARGET "11.0" CACHE STRING "macOS deployment target")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mmacosx-version-min=11.0")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -mmacosx-version-min=11.0")
endif()

# Signal that this is a Python build to avoid export issues
set(DFTRACER_PYTHON_BUILD ON CACHE BOOL "Building for Python" FORCE)

if (CMAKE_VERSION VERSION_LESS 3.18)
  set(DEV_MODULE Development)
else()
  set(DEV_MODULE Development.Module)
endif()

find_package(Python 3.8 
REQUIRED COMPONENTS Interpreter ${DEV_MODULE}
OPTIONAL_COMPONENTS Development.SABIModule)

execute_process(
  COMMAND "${Python_EXECUTABLE}" -m nanobind --cmake_dir
  OUTPUT_STRIP_TRAILING_WHITESPACE OUTPUT_VARIABLE nanobind_ROOT)
find_package(nanobind CONFIG REQUIRED)

if (NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build." FORCE)
  set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release" "MinSizeRel" "RelWithDebInfo")
endif()


# # # Create the package-style include directory structure in build directory
# set(PYREADER_INCLUDE_DIR ${CMAKE_CURRENT_BINARY_DIR}/include)
# file(MAKE_DIRECTORY ${PYREADER_INCLUDE_DIR}/dftracer_utils)
# file(COPY ${CMAKE_BINARY_DIR}/include DESTINATION ${PYREADER_INCLUDE_DIR}/dftracer_utils)

# set(READER_HEADERS
#     indexer.h
#     reader.h
#     platform_compat.h
#     filesystem.h
#     utils.h
# )

# foreach(header ${READER_HEADERS})
#     configure_file(
#         # ${CMAKE_CURRENT_SOURCE_DIR}/../reader/${header}
#         ${CMAKE_}
#         ${PYREADER_INCLUDE_DIR}/dftracer_utils/reader/${header}
#         COPYONLY
#     )
# endforeach()

if (APPLE)
  nanobind_add_module(
    dft_utils_reader_ext
    NB_STATIC STABLE_ABI
    src/reader_ext.cpp
  )
else()
  nanobind_add_module(
    dft_utils_reader_ext
    NB_STATIC STABLE_ABI LTO
    src/reader_ext.cpp
  )
endif()

# target_include_directories(dft_utils_reader_ext PRIVATE ${PYREADER_INCLUDE_DIR})
target_link_libraries(dft_utils_reader_ext PRIVATE dft_utils::shared)

# get_target_property(DFT_READER_LINK_LIBS dft_reader::static INTERFACE_LINK_LIBRARIES)
# if(DFT_READER_LINK_LIBS)
#     foreach(lib ${DFT_READER_LINK_LIBS})
#         if(TARGET ${lib})
#             get_target_property(LIB_INCLUDE_DIRS ${lib} INTERFACE_INCLUDE_DIRECTORIES)
#             if(LIB_INCLUDE_DIRS)
#                 target_include_directories(dft_reader_ext PRIVATE ${LIB_INCLUDE_DIRS})
#             endif()
#         endif()
#     endforeach()
# endif()

# Set RPATH for the extension module
if(APPLE)
  set_target_properties(dft_utils_reader_ext PROPERTIES
    INSTALL_RPATH "@loader_path;@loader_path/lib"
    BUILD_RPATH "@loader_path;@loader_path/lib"
    BUILD_WITH_INSTALL_RPATH FALSE
    INSTALL_RPATH_USE_LINK_PATH TRUE
  )
else()
  set_target_properties(dft_utils_reader_ext PROPERTIES
      INSTALL_RPATH_USE_LINK_PATH TRUE
      BUILD_WITH_INSTALL_RPATH TRUE
      INSTALL_RPATH "$ORIGIN;$ORIGIN/lib;$ORIGIN/lib64;$ORIGIN/../lib;$ORIGIN/../lib64;$ORIGIN/../../lib;$ORIGIN/dft_utils/lib;$ORIGIN/../dft_utils/lib"
  )
endif()

install(TARGETS dft_utils_reader_ext LIBRARY DESTINATION dft_utils)
