#include "mv_priv.h"
#include "mv/affinity.h"
#include "mv/macro.h"

MV_EXPORT
mvh* mv_hdl;

static void* ctx_data;
static mv_pool* mv_ctx_pool;

void* MPIV_HEAP;

void MPIV_Recv(void* buffer, int count, MPI_Datatype datatype,
    int rank, int tag,
    MPI_Comm comm __UNUSED__,
    MPI_Status* status __UNUSED__)
{
  int size;
  MPI_Type_size(datatype, &size);
  size *= count;
  struct mv_ctx ctx;
  while (!mv_recv(mv_hdl, buffer, size, rank, tag, &ctx))
    thread_yield();
  mv_sync* sync = mv_get_sync();
  mv_recv_post(mv_hdl, &ctx, sync);
  mv_wait(&ctx, sync);
}

void MPIV_Send(void* buffer, int count, MPI_Datatype datatype,
    int rank, int tag, MPI_Comm comm __UNUSED__)
{
  int size;
  MPI_Type_size(datatype, &size);
  size *= count;
  struct mv_ctx ctx;
  while (!mv_send(mv_hdl, buffer, size, rank, tag, &ctx))
    thread_yield();
  mv_sync* sync = mv_get_sync();
  mv_send_post(mv_hdl, &ctx, sync);
  mv_wait(&ctx, sync);
}

void MPIV_Ssend(void* buffer, int count, MPI_Datatype datatype,
    int rank, int tag, MPI_Comm comm __UNUSED__)
{
  int size;
  MPI_Type_size(datatype, &size);
  size *= count;
  struct mv_ctx ctx;
  while (!mv_send(mv_hdl, buffer, size, rank, tag, &ctx))
    thread_yield();
  mv_sync* sync = mv_get_sync();
  mv_send_post(mv_hdl, &ctx, sync);
  mv_wait(&ctx, sync);
}

void MPIV_Isend(const void* buf, int count, MPI_Datatype datatype, int rank,
                int tag, MPI_Comm comm __UNUSED__, MPIV_Request* req) {
  int size;
  MPI_Type_size(datatype, &size);
  size *= count;
  mv_ctx *ctx = (mv_ctx*) mv_pool_get(mv_ctx_pool);
  while (!mv_send(mv_hdl, buf, size, rank, tag, ctx))
    thread_yield();
  if (ctx->type != REQ_DONE) {
    ctx->complete = mv_send_post;
    *req = (MPIV_Request) ctx;
  } else {
    mv_pool_put(mv_ctx_pool, ctx);
    *req = MPI_REQUEST_NULL;
  }
}

void MPIV_Irecv(void* buffer, int count, MPI_Datatype datatype, int rank,
                int tag, MPI_Comm comm __UNUSED__, MPIV_Request* req) {
  int size;
  MPI_Type_size(datatype, &size);
  size *= count;
  mv_ctx *ctx = (mv_ctx*) mv_pool_get(mv_ctx_pool);
  while (!mv_recv(mv_hdl, (void*) buffer, size, rank, tag, ctx))
    thread_yield();
  ctx->complete = mv_recv_post;
  *req = (MPIV_Request) ctx;
}

void MPIV_Waitall(int count, MPIV_Request* req, MPI_Status* status __UNUSED__) {
  int pending = count;
  for (int i = 0; i < count; i++) {
    if (req[i] == MPI_REQUEST_NULL)
      pending--;
  }
  mv_sync* counter = mv_get_counter(pending);
  for (int i = 0; i < count; i++) {
    if (req[i] != MPI_REQUEST_NULL) {
      mv_ctx* ctx = (mv_ctx *) req[i];
      if (ctx->complete(mv_hdl, ctx, counter)) {
        thread_signal(counter);
      }
    }
  }
  for (int i = 0; i < count; i++) {
    if (req[i] != MPI_REQUEST_NULL) {
      mv_ctx* ctx = (mv_ctx*) req[i];
      mv_wait(ctx, counter);
      mv_pool_put(mv_ctx_pool, ctx);
      req[i] = MPI_REQUEST_NULL;
    }
  }
}

volatile int mv_thread_stop;
static pthread_t progress_thread;

static void* progress(void* arg __UNUSED__)
{
  set_me_to_last();
  while (!mv_thread_stop) {
    mv_progress(mv_hdl);
  }
  return 0;
}

void MPIV_Init(int* argc, char*** args)
{
  size_t heap_size = 256 * 1024 * 1024;
  mv_open(argc, args, heap_size, &mv_hdl);
  posix_memalign(&ctx_data, 64, sizeof(struct mv_ctx) * MAX_PACKET);
  mv_pool_create(&mv_ctx_pool);
  mv_ctx* ctxs = (mv_ctx*) ctx_data;
  for (int i = 0; i < MAX_PACKET; i++)
    mv_pool_put(mv_ctx_pool, &ctxs[i]);
  mv_thread_stop = 0;
  pthread_create(&progress_thread, 0, progress, 0);
  MPIV_HEAP = mv_heap_ptr(mv_hdl);
}

void MPIV_Finalize()
{
  mv_thread_stop = 1;
  pthread_join(progress_thread, 0);
  free(ctx_data);
  mv_close(mv_hdl);
}
