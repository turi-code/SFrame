project(lambda)

make_library(pylambda
  SOURCES
    lambda_constants.cpp
    lambda_master.cpp
    pylambda_function.cpp
    graph_pylambda_master.cpp
    # lualambda_master.cpp
  REQUIRES
    flexible_type cppipc sframe shmipc python_callbacks process
    # luastate luajit
    EXTERNAL_VISIBILITY
)


make_library(pylambda_worker
  SOURCES
  pylambda.cpp
  pylambda_worker.cpp
  graph_pylambda.cpp
  REQUIRES
    flexible_type cppipc sframe shmipc python_callbacks process
    EXTERNAL_VISIBILITY
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
        rcpplambda logger process RInside R ncurses
    )

    make_shared_library_from_static(rcpplambda_shared
      REQUIRES
        rcpplambda
      OUTPUT_NAME rcpplambda_shared
    )

endif()
