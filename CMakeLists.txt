cmake_minimum_required(VERSION 3.16)
project(glibc_aio_wrapper C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(PkgConfig REQUIRED)
pkg_check_modules(GLIB REQUIRED glib-2.0)

# --- ライブラリ ---
add_library(async_io STATIC async_io.c)
target_include_directories(async_io PUBLIC . ${GLIB_INCLUDE_DIRS})
target_link_libraries(async_io PUBLIC ${GLIB_LIBRARIES} rt pthread)
target_compile_definitions(async_io PUBLIC _GNU_SOURCE)
target_compile_options(async_io PRIVATE ${GLIB_CFLAGS_OTHER})

# --- テスト実行ファイル ---
add_executable(async_io_test tests/test_async_io.c)
target_link_libraries(async_io_test PRIVATE async_io)

# --- AddressSanitizer オプション ---
option(ENABLE_ASAN "Enable AddressSanitizer" OFF)
if(ENABLE_ASAN)
  target_compile_options(async_io PRIVATE -fsanitize=address)
  target_link_options(async_io PUBLIC -fsanitize=address)
endif()
