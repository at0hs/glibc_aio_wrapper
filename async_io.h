#pragma once

#include <glib-2.0/glib.h>

typedef struct RESULT {
  gint error;
  gssize return_value;
} RESULT;

typedef void (*ASYNC_IO_COMPLETED)(RESULT *result, void *user_data);

void async_io_init(gint pipe_no, void *(*allocate)(gsize size),
                   void (*free)(void *ptr));
void async_io_destroy(gint pipe_no);

gint async_io_write_async(gint fd, gconstpointer buf, gsize size,
                          goffset offset, ASYNC_IO_COMPLETED callback,
                          gpointer user_data);
gint async_io_write(gint fd, gpointer buf, gsize size, goffset offset);
void async_io_wait_operations_by_fd(gint fd);
