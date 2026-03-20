#include "async_io.h"

#include <aio.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdalign.h>
#include <string.h>
#include <unistd.h>

#define ASYNC_IO_MAX_CONTEXTS 2

typedef struct MEMORY_ALLOCATOR {
  void *(*allocate)(gsize size);
  void (*free)(void *ptr);
} MEMORY_ALLOCATOR;

typedef struct ASYNC_IO_CTX {
  pthread_t id;
  GMutex mutex;
  GCond cond;
  gint inflight;

  MEMORY_ALLOCATOR allocator;
  GQueue *io_requests;
} ASYNC_IO_CTX;

static ASYNC_IO_CTX contexts[ASYNC_IO_MAX_CONTEXTS] = {0};

static ASYNC_IO_CTX *get_myself(void) {
  for (gint i = 0; i < ASYNC_IO_MAX_CONTEXTS; i++) {
    if (contexts[i].id != 0 && pthread_equal(pthread_self(), contexts[i].id))
      return &contexts[i];
  }
  return NULL;
}

static inline ASYNC_IO_CTX *init_context(gint pipe_no) {
  ASYNC_IO_CTX *ctx = &contexts[pipe_no];
  ctx->id = pthread_self();
  return ctx;
}

typedef struct INFO {
  ASYNC_IO_CTX *ctx;
  struct aiocb aio;
  struct {
    ASYNC_IO_COMPLETED func;
    void *user_data;
  } callback;
} INFO;

static inline gint aio_wait(const struct aiocb *aio) {
  const struct aiocb *const aio_list[1] = {aio};
  if (aio_suspend(aio_list, 1, NULL) == -1)
    return -errno;
  return 0;
}

static inline void atomic_push_info(ASYNC_IO_CTX *ctx, INFO *info) {
  g_mutex_lock(&ctx->mutex);
  g_queue_push_tail(ctx->io_requests, info);
  ctx->inflight++;
  g_mutex_unlock(&ctx->mutex);
}

static inline void atomic_remove_info(ASYNC_IO_CTX *ctx, INFO *info) {
  g_mutex_lock(&ctx->mutex);
  g_queue_remove(ctx->io_requests, info);
  ctx->inflight--;
  g_cond_broadcast(&ctx->cond);
  g_mutex_unlock(&ctx->mutex);
}

static void io_completion_handler(union sigval sv) {
  INFO *info = sv.sival_ptr;
  ASYNC_IO_CTX *ctx = info->ctx;

  gint error = aio_error(&info->aio);
  gssize return_value = aio_return(&info->aio);
  g_print("IO status: %" G_GSSIZE_FORMAT ", error: %d\n", return_value, error);

  if (info->callback.func)
    info->callback.func(&(RESULT){.error = error, .return_value = return_value},
                        info->callback.user_data);

  ctx->allocator.free((gpointer)info->aio.aio_buf);
  atomic_remove_info(ctx, info);
  g_free(info);
}

static inline void wait_all_io_operations(ASYNC_IO_CTX *ctx) {
  g_mutex_lock(&ctx->mutex);
  while (ctx->inflight > 0)
    g_cond_wait(&ctx->cond, &ctx->mutex);
  g_mutex_unlock(&ctx->mutex);
}

void async_io_init(gint pipe_no, void *(*allocate)(gsize size),
                   void (*free)(void *ptr)) {
  g_assert(pipe_no >= 0 && pipe_no < ASYNC_IO_MAX_CONTEXTS);

  ASYNC_IO_CTX *ctx = init_context(pipe_no);
  g_assert(ctx->io_requests == NULL);

  ctx->allocator.allocate = allocate ? allocate : g_malloc;
  ctx->allocator.free = free ? free : g_free;
  ctx->io_requests = g_queue_new();
  ctx->inflight = 0;
  g_mutex_init(&ctx->mutex);
  g_cond_init(&ctx->cond);
}

void async_io_destroy(gint pipe_no) {
  g_assert(pipe_no >= 0 && pipe_no < ASYNC_IO_MAX_CONTEXTS);

  ASYNC_IO_CTX *ctx = &contexts[pipe_no];

  wait_all_io_operations(ctx);
  g_queue_free(ctx->io_requests);
  g_cond_clear(&ctx->cond);
  g_mutex_clear(&ctx->mutex);
  memset(ctx, 0, sizeof(*ctx));
}

gint async_io_write_async(gint fd, gconstpointer buf, gsize size,
                          goffset offset, ASYNC_IO_COMPLETED callback,
                          gpointer user_data) {
  ASYNC_IO_CTX *ctx = get_myself();
  g_assert(ctx != NULL);

  gpointer new_buf = ctx->allocator.allocate(size);
  if (!new_buf)
    return -ENOMEM;
  memcpy(new_buf, buf, size);

  INFO *info = g_malloc0(sizeof(*info));
  info->ctx = ctx;
  info->callback.func = callback;
  info->callback.user_data = user_data;

  info->aio.aio_fildes = fd;
  info->aio.aio_offset = offset;
  info->aio.aio_buf = new_buf;
  info->aio.aio_nbytes = size;
  info->aio.aio_sigevent.sigev_notify = SIGEV_THREAD;
  info->aio.aio_sigevent.sigev_value.sival_ptr = info;
  info->aio.aio_sigevent.sigev_notify_function = io_completion_handler;
  info->aio.aio_sigevent.sigev_notify_attributes = NULL;

  atomic_push_info(ctx, info);

  if (aio_write(&info->aio) == -1) {
    int err = errno;
    atomic_remove_info(ctx, info);
    ctx->allocator.free(new_buf);
    g_free(info);
    return -err;
  }
  return 0;
}

gint async_io_write(gint fd, gpointer buf, gsize size, goffset offset) {
  struct aiocb aio = {0};

  aio.aio_fildes = fd;
  aio.aio_offset = offset;
  aio.aio_buf = buf;
  aio.aio_nbytes = size;
  aio.aio_sigevent.sigev_notify = SIGEV_NONE;
  if (aio_write(&aio) == -1)
    return -errno;
  return aio_wait(&aio);
}

void async_io_wait_operations_by_fd(gint fd) {
  ASYNC_IO_CTX *ctx = get_myself();
  g_assert(ctx != NULL);

  g_mutex_lock(&ctx->mutex);
  for (;;) {
    gboolean found = FALSE;
    guint len = g_queue_get_length(ctx->io_requests);
    for (guint i = 0; i < len; i++) {
      INFO *info = g_queue_peek_nth(ctx->io_requests, i);
      if (info->aio.aio_fildes == fd) {
        found = TRUE;
        g_print("FD[%d] is processing\n", fd);
        break;
      }
    }
    if (!found) {
      g_print("FD[%d] is free\n", fd);
      break;
    }
    g_cond_wait(&ctx->cond, &ctx->mutex);
  }
  g_mutex_unlock(&ctx->mutex);
}
