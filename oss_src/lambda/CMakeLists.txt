project(lambda)

make_library(pylambda
  SOURCES
    lambda_constants.cpp
    python_api.cpp
    pylambda_function.cpp
    python_import_modules.cpp
    pyflexible_type.cpp
    pylambda.cpp
    lambda_master.cpp
    graph_pylambda.cpp
    graph_pylambda_master.cpp
    # lualambda_master.cpp
  REQUIRES
    python flexible_type cppipc sframe shmipc
    # luastate luajit
    EXTERNAL_VISIBILITY
)

make_executable(pylambda_worker
  SOURCES
    pylambda_worker.cpp
  REQUIRES
    pylambda tcmalloc logger process
)

if(${R_INTEGRATION} AND NOT WIN32)
# not build this on Windows now, since we have no idea it can pass or not
    make_library(rcpplambda
      SOURCES
        lambda_constants.cpp
        rcpplambda.cpp
        lambda_master.cpp
        rcpplambda_utils.cpp
      REQUIRES
        R RInside fileio flexible_type cppipc sframe shmipc
      EXTERNAL_VISIBILITY
    )

    make_executable(rcpplambda_worker
      SOURCES
        rcpplambda_worker.cpp
      REQUIRES
        rcpplambda tcmalloc logger process RInside R ncurses
    )

    make_shared_library_from_static(rcpplambda_shared
      REQUIRES
        rcpplambda
      OUTPUT_NAME rcpplambda_shared
    )

endif()

if(WIN32)
        # I could not get this to make a separate target without introducing 
        # alot of CMake ugliness later on. So this will have to do.
        add_custom_command(TARGET pylambda_worker POST_BUILD
                COMMENT "Produced stripped version of pylambda_worker.exe"
                COMMAND strip -s pylambda_worker.exe)
        add_custom_command(TARGET pylambda_worker POST_BUILD
                COMMENT "Add SxS manifest to pylambda_worker.exe"
                COMMAND cmd.exe /c "mt.exe /manifest ${CMAKE_CURRENT_SOURCE_DIR}/pylambda_worker.exe.manifest /outputresource:${CMAKE_CURRENT_BINARY_DIR}/pylambda_worker.exe;#1")
endif()
