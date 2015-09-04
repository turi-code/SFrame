project(luastate)

make_empty_library(luastate REQUIRES luajit dl)
# make_executable(state_test SOURCES state_test.cpp REQUIRES luastate)
