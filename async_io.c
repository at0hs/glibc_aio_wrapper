#include <aio.h>
#include <pthread.h>
#include <glib-2.0/glib.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdalign.h>

typedef struct MEMORY_ALLOCATOR {
	void *(*allocate)(gsize size);
	void (*free)(void *ptr);
} MEMORY_ALLOCATOR;
typedef struct RESULT {
	gint error;
	gint return_value;
} RESULT;
typedef void (*ASYNC_IO_COMPLETED)(RESULT *result, void *user_data);

typedef struct ASYNC_IO_CTX {
	pthread_t id;
	GMutex mutex;

	MEMORY_ALLOCATOR allocator;
	GQueue *io_requests;
} ASYNC_IO_CTX;

static ASYNC_IO_CTX contexts[2] = { 0 };
static ASYNC_IO_CTX *get_myself() {
	for (gint i = 0; i < 2; i++) {
		if (pthread_self() == contexts[i].id) return &contexts[i];
	}
	return NULL; // just in case
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

static inline gint wait(const struct aiocb *aio) {
	const struct aiocb *const aio_list[1] = { aio };
	if (aio_suspend(aio_list, 1, NULL) == -1) return -errno;
	return 0;
}

static inline void atomic_push_info(ASYNC_IO_CTX *ctx, INFO *info) {
	GMutexLocker *locker = g_mutex_locker_new(&ctx->mutex);
	g_queue_push_tail(ctx->io_requests, info);
	g_mutex_locker_free(locker);
}

static inline INFO *atomic_pop_info(ASYNC_IO_CTX *ctx) {
	INFO *info = NULL;
	GMutexLocker *locker = g_mutex_locker_new(&ctx->mutex);
	info = g_queue_pop_head(ctx->io_requests);
	g_mutex_locker_free(locker);
	return info;
}

static void io_completion_handler(ASYNC_IO_CTX *ctx) {
	INFO *info = atomic_pop_info(ctx);

	if (!info) return;

	// 念のため。。。
	while (aio_error(&info->aio) == EINPROGRESS) {
		g_usleep(100000);
	}
	gint error = aio_error(&info->aio);
	gint return_value = (gint)aio_return(&info->aio);
	g_print("IO status: %d, error: %d\n", return_value, aio_error(&info->aio));
	if (info->callback.func)
		info->callback.func(&(RESULT) { .error = error, .return_value = return_value }, info->callback.user_data);

	ctx->allocator.free((gpointer)info->aio.aio_buf);
	g_free(info);
}

static inline void wait_all_io_operations(ASYNC_IO_CTX *ctx) {
	while (g_queue_get_length(ctx->io_requests)) {
		g_usleep(100000);
	}
}

void async_io_init(gint pipe_no, void *(*allocate)(gsize size), void (*free)(void *ptr)) {
	ASYNC_IO_CTX *ctx = init_context(pipe_no);

	if (allocate) {
		ctx->allocator.allocate = allocate;
	} else {
		ctx->allocator.allocate = g_malloc;
	}
	if (free) {
		ctx->allocator.free = free;
	} else {
		ctx->allocator.free = g_free;
	}
	ctx->io_requests = g_queue_new();
	g_mutex_init(&ctx->mutex);
}

void async_io_destroy(gint pipe_no) {
	ASYNC_IO_CTX *ctx = &contexts[pipe_no];

	wait_all_io_operations(ctx);
	g_queue_free_full(ctx->io_requests, g_free);
	g_mutex_clear(&ctx->mutex);
	memset(ctx, 0, sizeof(*ctx));
}

gint async_io_write_async(gint fd, gconstpointer buf, gsize size, goffset offset, ASYNC_IO_COMPLETED callback, gpointer user_data) {
	ASYNC_IO_CTX *ctx = get_myself();

	gpointer new_buf = ctx->allocator.allocate(size);
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
	info->aio.aio_sigevent.sigev_value.sival_ptr = ctx;
	info->aio.aio_sigevent.sigev_notify_function = (void (*)(__sigval_t))io_completion_handler;
	info->aio.aio_sigevent.sigev_notify_attributes = NULL;

	atomic_push_info(ctx, info);
	return aio_write(&info->aio);
}

gint async_io_write(gint fd, gpointer buf, gsize size, goffset offset) {
	ASYNC_IO_CTX *ctx = get_myself();

	struct aiocb aio = { 0 };

	aio.aio_fildes = fd;
	aio.aio_offset = offset;
	aio.aio_buf = buf;
	aio.aio_nbytes = size;
	aio.aio_sigevent.sigev_notify = SIGEV_NONE;
	if (aio_write(&aio) == -1) {
		return -errno;
	}
	return wait(&aio);
}

void async_io_wait_operations_by_fd(gint fd) {
	ASYNC_IO_CTX *ctx = get_myself();
	gboolean io_processing = FALSE;

	while (TRUE) {
		GMutexLocker *locker = g_mutex_locker_new(&ctx->mutex);
		guint len = g_queue_get_length(ctx->io_requests);
		for (guint i = 0; i < len; i++) {
			INFO *info = g_queue_peek_nth(ctx->io_requests, i);
			if (info->aio.aio_fildes == fd) {
				io_processing = TRUE;
				g_print("FD[%d] is processing\n", fd);
				break;
			}
		}
		g_mutex_locker_free(locker);
		if (!io_processing) {
			g_print("FD[%d] is free\n", fd);
			break;
		}
		io_processing = FALSE;
		g_usleep(100000);
	}
}

void test_simple_async_write() {
	async_io_init(0, NULL, NULL);

	int fd = open("simple_async.txt", O_WRONLY | O_CREAT | O_TRUNC, 0644);
	g_assert_cmpint(fd, > , 0);

	char msg[] = "Hello World";

	gint ret = async_io_write_async(fd, msg, sizeof(msg), 0, NULL, NULL);
	g_assert_cmpint(0, == , ret);

	async_io_wait_operations_by_fd(fd);
	close(fd);
	async_io_destroy(0);
}

void test_simple_sync_write() {
	async_io_init(0, NULL, NULL);
	int fd = open("simple_write.txt", O_WRONLY | O_CREAT | O_TRUNC, 0644);
	g_assert_cmpint(fd, > , 0);

	char msg[] = "Hello World";

	gint ret = async_io_write(fd, msg, sizeof(msg), 0);
	g_assert_cmpint(0, == , ret);

	async_io_wait_operations_by_fd(fd);
	close(fd);
	async_io_destroy(0);
}

void test_direct_io_passing_no_aligned_memory() {
	async_io_init(0, NULL, NULL);

	int fd = open("direct_io_failed.bin", O_WRONLY | O_CREAT | O_TRUNC | __O_DIRECT, 0644);
	g_assert_cmpint(fd, > , 0);

	char msg[] = "Hello World";

	gint ret = async_io_write_async(fd, msg, sizeof(msg), 0, NULL, NULL);
	g_assert_cmpint(0, == , ret);

	async_io_wait_operations_by_fd(fd);
	close(fd);
	async_io_destroy(0);
}

void *custom_aligned_allocate(gsize size) {
	void *ptr = aligned_alloc(4096, size);
	return ptr;
}

void test_direct_io() {
	async_io_init(0, custom_aligned_allocate, free);

	int fd = open("direct_io_success.bin", O_WRONLY | O_CREAT | O_TRUNC | __O_DIRECT, 0644);
	g_assert_cmpint(fd, > , 0);

	void *dummy = g_malloc(4096);
	g_assert(dummy != NULL);

	gint ret = async_io_write_async(fd, dummy, 4096, 0, NULL, NULL);
	g_assert_cmpint(0, == , ret);
	g_free(dummy);
	async_io_wait_operations_by_fd(fd);
	close(fd);
	async_io_destroy(0);
}

void callback(RESULT *result, void *user_data) {
	*(RESULT *)user_data = *result;
}

void test_callback() {
	RESULT result;
	async_io_init(0, NULL, NULL);

	int fd = open("callback.bin", O_WRONLY | O_CREAT | O_TRUNC | __O_DIRECT, 0644);
	g_assert_cmpint(fd, > , 0);

	char msg[] = "Hello World";

	gint ret = async_io_write_async(fd, msg, sizeof(msg), 0, callback, &result);
	g_assert_cmpint(0, == , ret);

	async_io_wait_operations_by_fd(fd);
	close(fd);
	async_io_destroy(0);
	g_assert_cmpint(result.error, == , 0);
	g_assert_cmpint(result.return_value, == , sizeof(msg));
}

int main() {
	// test_simple_async_write();
	test_simple_sync_write();
	// test_direct_io_passing_no_aligned_memory();
	// test_direct_io();
	return 0;
}
