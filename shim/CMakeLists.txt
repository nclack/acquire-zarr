cmake_minimum_required(VERSION 3.18)
project(acquire-zarr-shim LANGUAGES C CXX)

set(CMAKE_C_STANDARD 11)
set(CMAKE_CXX_STANDARD 17)

enable_testing()
include(CTest)

# Chucky's CMakeLists.txt uses CMAKE_SOURCE_DIR for its cmake module path,
# which breaks when it's a subdirectory. Prepend the correct path here.
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/chucky/cmake")

add_subdirectory(chucky)

# --- shim library --------------------------------------------------------

add_library(acquire-zarr-chucky-cpu STATIC
    shim.c
    shim_convert.c
    shim_sink.c
)

target_include_directories(acquire-zarr-chucky-cpu
    PUBLIC  ${CMAKE_CURRENT_SOURCE_DIR}/../include
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(acquire-zarr-chucky-cpu PRIVATE
    stream_cpu multiarray_cpu
    store_fs zarr_array zarr_group ngff_multiscale
    hcs hcs_metadata
    dimension writer stream_config
    platform chucky_log
)

target_compile_definitions(acquire-zarr-chucky-cpu PRIVATE
    ACQUIRE_ZARR_API_VERSION="0.6.0"
)

set_target_properties(acquire-zarr-chucky-cpu PROPERTIES
    POSITION_INDEPENDENT_CODE ON
)

# --- logger compat lib for integration test macros -----------------------

add_library(shim-test-logger STATIC
    compat/logger.cpp
)

target_include_directories(shim-test-logger PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/compat
)

set_target_properties(shim-test-logger PROPERTIES
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED ON
)

# --- integration tests ---------------------------------------------------

find_package(nlohmann_json CONFIG QUIET)

if(nlohmann_json_FOUND)
    set(integration_tests
        stream-raw-to-filesystem
        stream-named-array-to-filesystem
        stream-compressed-to-filesystem
        stream-zstd-compressed-to-filesystem
        stream-2d-multiscale-to-filesystem
        stream-3d-multiscale-to-filesystem
        stream-multi-frame-append
        stream-multiscale-trivial-3rd-dim
        stream-multiple-arrays-to-filesystem
        estimate-memory-usage
        stream-pure-hcs-acquisition
        stream-mixed-flat-and-hcs-acquisition
        stream-with-ragged-final-shard
    )
    # Excluded: stream-append-nullptr (depends on miniocpp)

    # S3 tests depend on miniocpp/client.h from the original acquire-zarr build.
    # Excluded until the shim's S3 path is implemented and tests are adapted.
    # set(s3_tests
    #     stream-raw-to-s3
    #     stream-named-array-to-s3
    #     stream-compressed-to-s3
    # )

    foreach(name ${integration_tests})
        set(tgt "shim-test-${name}")
        add_executable(${tgt}
            ${CMAKE_CURRENT_SOURCE_DIR}/../tests/integration/${name}.cpp
        )
        target_compile_definitions(${tgt} PRIVATE "TEST=\"${tgt}\"")
        target_include_directories(${tgt} PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/../include
        )
        target_link_libraries(${tgt} PRIVATE
            acquire-zarr-chucky-cpu
            shim-test-logger
            nlohmann_json::nlohmann_json
        )
        set_target_properties(${tgt} PROPERTIES
            CXX_STANDARD 20
            CXX_STANDARD_REQUIRED ON
        )
        add_test(NAME ${tgt} COMMAND ${tgt})

        set(labels "shim;integration")
        if(name MATCHES ".*s3.*")
            list(APPEND labels "s3")
        endif()
        set_tests_properties(${tgt} PROPERTIES LABELS "${labels}")
    endforeach()
else()
    message(STATUS "nlohmann_json not found -- skipping integration tests")
endif()
