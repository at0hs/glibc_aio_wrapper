#include "async_io.h"

#include <fcntl.h>
#include <glib-2.0/glib.h>
#include <stdlib.h>
#include <unistd.h>

void test_simple_async_write(void) {
  async_io_init(0, NULL, NULL);

  int fd = open("simple_async.txt", O_WRONLY | O_CREAT | O_TRUNC, 0644);
  g_assert_cmpint(fd, >, 0);

  char msg[] = "Hello World";

  gint ret = async_io_write_async(fd, msg, sizeof(msg), 0, NULL, NULL);
  g_assert_cmpint(0, ==, ret);

  async_io_wait_operations_by_fd(fd);
  close(fd);
  async_io_destroy(0);
}

void test_simple_sync_write(void) {
  async_io_init(0, NULL, NULL);
  int fd = open("simple_write.txt", O_WRONLY | O_CREAT | O_TRUNC, 0644);
  g_assert_cmpint(fd, >, 0);

  char msg[] = "Hello World";

  gint ret = async_io_write(fd, msg, sizeof(msg), 0);
  g_assert_cmpint(0, ==, ret);

  close(fd);
  async_io_destroy(0);
}

void test_direct_io_passing_no_aligned_memory(void) {
  async_io_init(0, NULL, NULL);

  int fd = open("direct_io_failed.bin",
                O_WRONLY | O_CREAT | O_TRUNC | __O_DIRECT, 0644);
  g_assert_cmpint(fd, >, 0);

  char msg[] = "Hello World";

  gint ret = async_io_write_async(fd, msg, sizeof(msg), 0, NULL, NULL);
  g_assert_cmpint(0, ==, ret);

  async_io_wait_operations_by_fd(fd);
  close(fd);
  async_io_destroy(0);
}

static void *custom_aligned_allocate(gsize size) {
  gsize aligned_size = (size + 4095) & ~(gsize)4095;
  return aligned_alloc(4096, aligned_size);
}

void test_direct_io(void) {
  async_io_init(0, custom_aligned_allocate, free);

  int fd = open("direct_io_success.bin",
                O_WRONLY | O_CREAT | O_TRUNC | __O_DIRECT, 0644);
  g_assert_cmpint(fd, >, 0);

  void *dummy = g_malloc(4096);
  g_assert(dummy != NULL);

  gint ret = async_io_write_async(fd, dummy, 4096, 0, NULL, NULL);
  g_assert_cmpint(0, ==, ret);
  g_free(dummy);
  async_io_wait_operations_by_fd(fd);
  close(fd);
  async_io_destroy(0);
}

static void callback(RESULT *result, void *user_data) {
  *(RESULT *)user_data = *result;
}

void test_callback(void) {
  RESULT result = {0};
  async_io_init(0, NULL, NULL);

  int fd =
      open("callback.bin", O_WRONLY | O_CREAT | O_TRUNC | __O_DIRECT, 0644);
  g_assert_cmpint(fd, >, 0);

  char msg[] = "Hello World";

  gint ret = async_io_write_async(fd, msg, sizeof(msg), 0, callback, &result);
  g_assert_cmpint(0, ==, ret);

  async_io_wait_operations_by_fd(fd);
  close(fd);
  async_io_destroy(0);
  g_assert_cmpint(result.error, ==, 0);
  g_assert_cmpint(result.return_value, ==, (gssize)sizeof(msg));
}

int main(void) {
  // test_simple_async_write();
  test_simple_sync_write();
  // test_direct_io_passing_no_aligned_memory();
  // test_direct_io();
  return 0;
}
