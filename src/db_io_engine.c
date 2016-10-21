#include "redis.h"
#include "thread_pool.h"
#include "checkpoint.h"
#include "dbe_get.h"
#include "bl_ctx.h"

#include "ds_log.h"

#include <unistd.h>

/* [0]:dbe_get, [1]:dbe_set, [2]:write_bl */
thread_pool_t *sc_pool[3];

typedef void (*routine_func)(void *ptr);
typedef struct user_data_t
{
    int send_fd;
    routine_func rf;
    thread_pool_t *pool;
    void *ctx;
    int cmd;
} user_data;

static void readFromDbIo(aeEventLoop *el, int fd, void *privdata, int mask);
//static void db_do_io_result(aeEventLoop *el, int fd, void *privdata, int mask);
static int io_worker_action(void **private, queue_data_t *task);
static queue_data_t *gen_db_io_task(routine_func rf, thread_pool_t *pool, int fd, void *ctx, int cmd);
static int commit_db_io_task(void *ctx, routine_func rf, int cmd, thread_pool_t *, int);
static void dbe_set_task_routine(void *ptr);
static void dbe_get_task_routine(void *ptr);
static void write_bl_task_routine(void *ptr);
extern int notify_main_server(int fd, const void *buf, size_t buf_size);

#ifndef _UPD_DBE_BY_PERIODIC_
static pthread_t tid;
static pthread_t wr_bl_tid;
static pthread_t wr_dbe_tid;
extern char gWrblRunning;
#endif

static sds rsp_buf;

int db_io_init()
{
    int fds[2];
    if (pipe(fds) == -1)
    {
        log_error("pipe() fail: %s", strerror(errno));
        return -1;
    }
    server.recv_fd = fds[0];
    server.send_fd = fds[1];
    rsp_buf = sdsempty();

    if (aeCreateFileEvent(server.el, server.recv_fd, AE_READABLE, readFromDbIo, 0)
        == AE_ERR)
    {
        log_error("%s", "aeCreateFileEvent() fail");
        return -1;
    }

    if (server.has_dbe == 1)
    {
    sc_pool[0] = create_thread_pool("DBE_GET", 10240, TOW_QUEUE);
    if (sc_pool[0] == 0)
    {
        log_error("create_thread_pool() fail for dbe_get, pool_size=%d", 10240);
        return -1;
    }
    if (add_thread_pool_worker(sc_pool[0], "io_worker_get", 0, io_worker_action, 0, 2) < 0)
    {
        log_error("add_thread_pool_worker() fail, thread_cnt=%d", 2);
        return -1;
    }
    }

#ifdef _UPD_DBE_BY_PERIODIC_
    if (server.has_dbe == 1)
    {
    sc_pool[1] = create_thread_pool("DBE_SET", 2, TOW_QUEUE);
    if (sc_pool[1] == 0)
    {
        log_error("create_thread_pool() fail for dbe_set, pool_size=%d", 2);
        return -1;
    }
    if (add_thread_pool_worker(sc_pool[1], "IO_WORKER_SET", 0, io_worker_action, 0, 1) < 0)
    {
        log_error("add_thread_pool_worker() fail, thread_cnt=%d", 1);
        return -1;
    }
    }

    sc_pool[2] = create_thread_pool("WRITE_BL", 2, TOW_QUEUE);
    if (sc_pool[2] == 0)
    {
        log_error("create_thread_pool() fail for write_bl, pool_size=%d", 2);
        return -1;
    }
    if (add_thread_pool_worker(sc_pool[2], "IO_WORKER_BL", 0, io_worker_action, 0, 1) < 0)
    {
        log_error("add_thread_pool_worker() fail, thread_cnt=%d", 1);
        return -1;
    }
#else
    int ret;
    do
    {
        ret = pthread_create(&tid, 0, do_handle_write_bl, 0);
        if (ret != 0)
        {
            log_error("%s", "pthread_create() fail handle_bl");
        }
    } while (ret);
    do
    {
        ret = pthread_create(&wr_bl_tid, 0, do_write_bl, 0);
        if (ret != 0)
        {
            log_error("%s", "pthread_create() fail write_bl");
        }
    } while (ret);
    if (server.has_dbe == 1)
    {
    do
    {
        ret = pthread_create(&wr_dbe_tid, 0, do_write_dbe, 0);
        if (ret != 0)
        {
            log_error("%s", "pthread_create() fail write_dbe");
        }
    } while (ret);
    }

    log_prompt("handle_bl=%lu, wr_bl=%lu, wr_dbe=%lu" , tid, wr_bl_tid, wr_dbe_tid);
#endif

    log_prompt("db_io_init() succ, pool_g=%p, pool_s=%p, pool_w=%p, recv_fd=%d, send_fd=%d"
              , sc_pool[0], sc_pool[1], sc_pool[2], fds[0], fds[1]);

    return 0;
}

void db_io_uninit()
{
#ifndef _UPD_DBE_BY_PERIODIC_
    log_prompt("db_io_uninit: handle_bl=%lu, wr_bl=%lu, wr_dbe=%lu"
            , tid, wr_bl_tid, wr_dbe_tid);
    gWrblRunning = 0;
    void *ret = 0;
    pthread_join(tid, &ret);
    pthread_join(wr_bl_tid, &ret);
    if (server.has_dbe == 1)
    {
        pthread_join(wr_dbe_tid, &ret);
    }
    log_prompt("db_io_uninit finish");
#endif
}

#if 0
static void db_do_io_result(aeEventLoop *el, int fd, void *privdata, int mask)
{
    log_test("db_do_io_result()...fd=%d, sc_pool=%p", fd, sc_pool);

    REDIS_NOTUSED(el);
    REDIS_NOTUSED(mask);
    REDIS_NOTUSED(privdata);

    char req[1];
    if (read(fd, req, 1) != 1)
    {
        log_warn("read() fail, fd=%d, recv_fd=%d", fd, server.recv_fd);
        return;
    }

    const int idx = req[0] - '0';
    if (idx > 3 || idx < 0)
    {
        log_warn("invalid notify: %c", req[0]);
        return;
    }

    void *ctx = 0;
    int cmd = idx;

    if (idx != 3)
    {
        queue_data_t *task = get_resp(sc_pool[idx], QUEUE_NONBLOCK);
        if (task == 0)
        {
            log_error("%s", "get_resp() fail, return NULL");
            return;
        }
        user_data *ud = (user_data *)task->data;
        ctx = ud->ctx;
        cmd = ud->cmd;
        zfree(task);
    }

    /* do with the transaction */
    if (cmd == 0)
    {
        after_dbe_set(ctx);
    }
    else if (cmd == 1)
    {
        after_dbe_get(ctx);
    }
    else if (cmd == 2)
    {
        g_tag = 0;
        after_write_bl((void *)ctx);
    }
    else if (cmd == 3)
    {
        g_tag = 0;
        after_write_bl(NULL);
    }
    else
    {
        log_error("unknown cmd from thread-pool, cmd=%d", cmd);
    }

    g_tag = -1;
    g_tid = -1;

    return;
}
#endif

static void db_do_io_result();
static void readFromDbIo(aeEventLoop *el, int fd, void *privdata, int mask)
{
    char buf[REDIS_IOBUF_LEN];
    int nread;
    REDIS_NOTUSED(el);
    REDIS_NOTUSED(mask);
    REDIS_NOTUSED(privdata);

    nread = read(fd, buf, REDIS_IOBUF_LEN);
    if (nread == -1)
    {
        if (errno == EAGAIN) {
            nread = 0;
        } else {
            log_error("read() fail from fd=%d: %s", fd, strerror(errno));
            return;
        }
    } else if (nread == 0) {
        log_info("fd=%d closed", fd);
        return;
    }
    if (nread) {
        rsp_buf = sdscatlen(rsp_buf, buf, nread);
    } else {
        return;
    }
    db_do_io_result();
}

static void db_do_io_result()
{
    int buf_len = sdslen(rsp_buf);
    char *ptr = rsp_buf;
    while (buf_len > 0)
    {
        if (*ptr == '0')
        {
            if (buf_len < 10)
            {
                log_warn("invalid input_len=%d for cmd(%c)", buf_len, *ptr);
                break;
            }
            void *ctx = 0;
            const int len = *(ptr + 1);
            if (len > 8)
            {
                log_warn("invalid arg_len=%d for cmd(%c), ignore this cmd", len, *ptr);
                ptr += 10;
                buf_len -= 10;
                continue;
            }
            memcpy(&ctx, ptr + 2, len);
            //log_debug("ctx=%p, arg_len=%d", ctx, len);
            ptr += 10;
            buf_len -= 10;
            after_dbe_get(ctx);
        }
        else if (*rsp_buf == '3')
        {
            ptr++;
            buf_len--;
            after_write_bl(NULL);
        }
        else
        {
            log_error("invalid cmd=%c(%d)", *ptr, *(unsigned char*)ptr);
            ptr++;
            buf_len--;
        }
    }
    rsp_buf = sdsrange(rsp_buf, ptr - rsp_buf, -1);

    return;
}

/* call by thread in thread_pool */
static int io_worker_action(void **private, queue_data_t *task)
{
    REDIS_NOTUSED(private);

    user_data *ud = (user_data *)task->data;
    //dbmng_ctx *ctx = (dbmng_ctx *)ud->ctx;

    log_debug2(0, "io_worker_action()...task=%p, pool=%p, ctx=%p, cmd=%d",
               task, ud->pool, ud->ctx, ud->cmd);

    ud->rf(task);

    return 0;
}

int commit_dbe_set_task(void *ctx)
{
    log_debug("commit_dbe_set_task(), ctx=%p", ctx);
    return commit_db_io_task(ctx, dbe_set_task_routine, 1, sc_pool[1], QUEUE_BLOCKING);
}

int commit_dbe_get_task(void *ctx)
{
    log_debug("commit_dbe_get_task(), ctx=%p", ctx);
    return commit_db_io_task(ctx, dbe_get_task_routine, 0, sc_pool[0], QUEUE_NONBLOCK);
}

int commit_write_bl_task(void *ctx)
{
    log_debug("commit_write_bl_task(), ctx=%p", ctx);
    return commit_db_io_task(ctx, write_bl_task_routine, 2, sc_pool[2], QUEUE_BLOCKING);
}

// block: QUEUE_NONBLOCK, QUEUE_BLOCKING
static int commit_db_io_task(void *ctx, routine_func rf, int cmd, thread_pool_t *pool, int block)
{
    queue_data_t *task = 0;
    task = gen_db_io_task(rf, pool, server.send_fd, ctx, cmd);
    if (task == 0)
    {
        return -1;
    }

    const int ret = put_req(pool, task, block);
    if (ret != 0)
    {
        //log_error("put_req() fail,ret=%d", ret);
        zfree(task);
        return -1;
    }

    log_debug("commit_db_io_task() succ, ctx=%p, pool=%p", ctx, pool);

    return 0;
}

static queue_data_t *gen_db_io_task(routine_func rf, thread_pool_t *pool, int fd, void *ctx, int cmd)
{
    queue_data_t *task = (queue_data_t *)zmalloc(sizeof(queue_data_t) + sizeof(user_data));
    if (task == 0)
    {
        log_error("zmalloc() fail for queue_data_t, fd=%d", fd);
        return 0;
    }

    //task->routine_cb = rf;
    task->data = (void *)((char *)task + sizeof(*task));

    user_data *ud = (user_data *)task->data;
    ud->rf = rf;
    ud->pool = pool;
    ud->send_fd = fd;
    ud->ctx = ctx;
    ud->cmd = cmd;

    return task;
}

/* call by thread in thread_pool */
static void dbe_set_task_routine(void *ptr)
{
    queue_data_t *task = (queue_data_t *)ptr;
    user_data *ud = (user_data *)task->data;

    log_debug("dbe_set_task running...pool=%p, ctx=%p", ud->pool, ud->ctx);
    do_dbe_set((dbmng_ctx *)ud->ctx);

    /* finish action, return to notice main server */
    const int fd = ud->send_fd;
    //put_resp(ud->pool, task, QUEUE_BLOCKING);
    notify_main_server(fd, "1", 1);
}

static void dbe_get_task_routine(void *ptr)
{
    queue_data_t *task = (queue_data_t *)ptr;
    user_data *ud = (user_data *)task->data;
    void *ctx = ud->ctx;

    log_debug("dbe_get_task running...pool=%p, ctx=%p", ud->pool, ctx);
    do_dbe_get(ctx);

    /* finish action, return to notice main server */
    const int fd = ud->send_fd;
    //put_resp(ud->pool, task, QUEUE_BLOCKING);
    //notify_main_server(fd, "0", 1);
    char buf[10];
    buf[0] = '0';
    buf[1] = sizeof(ctx);
    memcpy(buf + 2, &ctx, buf[1]);
    notify_main_server(fd, buf, sizeof(buf));
}

static void write_bl_task_routine(void *ptr)
{
    queue_data_t *task = (queue_data_t *)ptr;
    user_data *ud = (user_data *)task->data;

    log_debug("write_bl_task running...pool=%p, ctx=%p", ud->pool, ud->ctx);
    do_handle_write_bl(ud->ctx);

    /* finish action, return to notice main server */
    const int fd = ud->send_fd;
    //put_resp(ud->pool, task, QUEUE_BLOCKING);
    notify_main_server(fd, "2", 1);
}

int notify_main_server(int fd, const void *buf, size_t buf_size)
{
    ssize_t ret;
    if ((ret = write(fd, buf, buf_size)) != (ssize_t)buf_size)
    {
        log_error("notify_main_thread: write %zdB to %d fail, ret=%d"
                , buf_size, fd, ret);
        return -1;
    }
    else
    {
        //log_debug2(tag, "write cmd(\"%s\") succ", "0");
        return 0;
    }
}

#ifndef _UPD_DBE_BY_PERIODIC_
void wakeupWrbl()
{
    notify_main_server(server.send_fd, "3", 1);
}
#endif

