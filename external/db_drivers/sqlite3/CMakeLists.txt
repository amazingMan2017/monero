if(FREEBSD)
    add_definitions(-DMDB_DSYNC=O_SYNC)
endif()

if(ANDROID)
    add_definitions("-DANDROID=1")
endif()

add_definitions( -DSQLITE_HAS_CODEC)
set (sqlite3_sources sqlite3.c)

include_directories("${CMAKE_CURRENT_SOURCE_DIR}")

add_library(sqlite3 ${sqlite3_sources})
target_link_libraries(sqlite3 PRIVATE ${OPENSSL_LIBRARIES})

if(${ARCH_WIDTH} EQUAL 32)
    target_compile_definitions(sqlite3 PUBLIC -DMDB_VL32)
endif()

# GUI/libwallet install target
if (BUILD_GUI_DEPS)
    if(IOS)
        set(lib_folder lib-${ARCH})
    else()
        set(lib_folder lib)
    endif()
    install(TARGETS sqlite3
            ARCHIVE DESTINATION ${lib_folder}
            LIBRARY DESTINATION ${lib_folder})
endif()
set_property(TARGET sqlite3 APPEND PROPERTY COMPILE_FLAGS "-fPIC")