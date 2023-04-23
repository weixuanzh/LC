#include "runtime/lcii.h"

static inline LCI_error_t LCII_progress_bq(LCI_device_t device)
{
  if (LCII_bq_is_empty(&device->bq)) return LCI_ERR_RETRY;
  if (!LCIU_try_acquire_spinlock(&device->bq_spinlock)) return LCI_ERR_RETRY;
  LCI_error_t ret = LCI_ERR_RETRY;
  LCII_bq_entry_t* entry = LCII_bq_top(&device->bq);
  if (entry != NULL) {
    LCII_PCOUNTERS_WRAPPER(
        LCII_pcounters[LCIU_get_thread_id()].backlog_queue_send_attempts++);
    if (entry->bqe_type == LCII_BQ_SENDS) {
      ret = LCIS_post_sends(device->endpoint_progress.endpoint, entry->rank,
                            entry->buf, entry->size, entry->meta);
    } else if (entry->bqe_type == LCII_BQ_SEND) {
      ret = LCIS_post_send(device->endpoint_progress.endpoint, entry->rank,
                           entry->buf, entry->size, entry->mr, entry->meta,
                           entry->ctx);
    } else if (entry->bqe_type == LCII_BQ_PUT) {
      ret = LCIS_post_put(device->endpoint_progress.endpoint, entry->rank,
                          entry->buf, entry->size, entry->mr, entry->base,
                          entry->offset, entry->rkey, entry->ctx);
    } else if (entry->bqe_type == LCII_BQ_PUTIMM) {
      ret =
          LCIS_post_putImm(device->endpoint_progress.endpoint, entry->rank,
                           entry->buf, entry->size, entry->mr, entry->base,
                           entry->offset, entry->rkey, entry->meta, entry->ctx);
    } else {
      LCM_DBG_Assert(false, "Unknown bqe_type (%d)!\n", entry->bqe_type);
    }
    if (ret == LCI_OK) {
      LCM_DBG_Log(LCM_LOG_DEBUG, "bq", "Pop from backlog queue: type %d\n",
                  entry->bqe_type);
      LCII_bq_pop(&device->bq);
      if (entry->bqe_type == LCII_BQ_SENDS) LCIU_free(entry->buf);
    }
  }
  LCIU_release_spinlock(&device->bq_spinlock);
  return ret;
}

LCI_error_t LCII_poll_cq(LCII_endpoint_t* endpoint)
{
  int ret = LCI_ERR_RETRY;
  // poll progress endpoint completion queue
  LCIS_cq_entry_t entry[LCI_CQ_MAX_POLL];
  int count = LCIS_poll_cq(endpoint->endpoint, entry);
  if (count > 0) {
    ret = LCI_OK;
  } else {
    LCM_DBG_Assert(count >= 0, "ibv_poll_cq returns error %d\n", count);
  }
  for (int i = 0; i < count; i++) {
#ifdef LCI_ENABLE_SLOWDOWN
    LCIU_spin_for_nsec(LCI_RECV_SLOW_DOWN_USEC * 1000);
#endif
    if (entry[i].opcode == LCII_OP_RECV) {
      // two-sided recv.
      LCM_DBG_Log(LCM_LOG_DEBUG, "device",
                  "complete recv: packet %p rank %d length %lu imm_data %u\n",
                  entry[i].ctx, entry[i].rank, entry[i].length,
                  entry[i].imm_data);
      LCIS_serve_recv((LCII_packet_t*)entry[i].ctx, entry[i].rank,
                      entry[i].length, entry[i].imm_data);
#ifdef LCI_ENABLE_MULTITHREAD_PROGRESS
      atomic_fetch_sub_explicit(&endpoint->recv_posted, 1,
                                LCIU_memory_order_relaxed);
#else
      --endpoint->recv_posted;
#endif
    } else if (entry[i].opcode == LCII_OP_RDMA_WRITE) {
      LCM_DBG_Log(LCM_LOG_DEBUG, "device", "complete write: imm_data %u\n",
                  entry[i].imm_data);
      if (entry[i].ctx != NULL) {
        LCII_free_packet((LCII_packet_t*)entry[i].ctx);
#ifdef LCI_ENABLE_MULTITHREAD_PROGRESS
        atomic_fetch_sub_explicit(&endpoint->recv_posted, 1,
                                  LCIU_memory_order_relaxed);
#else
        --endpoint->recv_posted;
#endif
      }
      LCIS_serve_rdma(entry[i].imm_data);
    } else {
      // entry[i].opcode == LCII_OP_SEND
      LCM_DBG_Log(LCM_LOG_DEBUG, "device", "complete send: address %p\n",
                  (void*)entry[i].ctx);
      if (entry[i].ctx == NULL) continue;
      LCIS_serve_send((void*)entry[i].ctx);
    }
  }
  return ret;
}

LCI_error_t LCII_fill_rq(LCII_endpoint_t* endpoint, bool block)
{
  int ret = LCI_ERR_RETRY;
#ifdef LCI_ENABLE_MULTITHREAD_PROGRESS
  while (atomic_load_explicit(&endpoint->recv_posted, memory_order_relaxed) <
         LCI_SERVER_MAX_RECVS) {
#else
  while (endpoint->recv_posted < LCI_SERVER_MAX_RECVS) {
#endif
    LCII_packet_t* packet = LCII_alloc_packet_nb(endpoint->device->pkpool);
    if (packet == NULL) {
      LCII_PCOUNTERS_WRAPPER(
          LCII_pcounters[LCIU_get_thread_id()].recv_backend_no_packet++);
      if (block) {
        // Try again
        continue;
      } else {
        break;
      }
    } else {
      // TODO: figure out what is the right poolid to set
      packet->context.poolid = lc_pool_get_local(endpoint->device->pkpool);
      LCIS_post_recv(endpoint->endpoint, packet->data.address, LCI_MEDIUM_SIZE,
                     endpoint->device->heap.segment->mr, packet);
#ifdef LCI_ENABLE_MULTITHREAD_PROGRESS
      atomic_fetch_add_explicit(&endpoint->recv_posted, 1,
                                LCIU_memory_order_relaxed);
#else
      ++endpoint->recv_posted;
#endif
      ret = LCI_OK;
    }
  }
  return ret;
}

LCI_error_t LCI_progress(LCI_device_t device)
{
  int ret = LCI_ERR_RETRY;
  // we want to make progress on the endpoint_progress as much as possible
  // to speed up rendezvous protocol
  while (LCII_poll_cq(&device->endpoint_progress) == LCI_OK) {
    ret = LCI_OK;
  }
  while (LCII_progress_bq(device) == LCI_OK) {
    ret = LCI_OK;
  }
  // Make sure we always have enough packet, but do not block.
  if (LCII_fill_rq(&device->endpoint_progress, false) == LCI_OK) {
    ret = LCI_OK;
  }
  if (LCII_poll_cq(&device->endpoint_worker) == LCI_OK) {
    ret = LCI_OK;
  }
  // Make sure we always have enough packet, but do not block.
  if (LCII_fill_rq(&device->endpoint_worker, false) == LCI_OK) {
    ret = LCI_OK;
  }
  LCII_PCOUNTERS_WRAPPER(LCII_pcounters[LCIU_get_thread_id()].progress_call +=
                         1);
#ifdef LCI_USE_PERFORMANCE_COUNTER
  if (ret == LCI_OK) {
    ++device->did_work_consecutive;
    LCII_pcounters[LCIU_get_thread_id()].progress_useful_call++;
  } else {
    LCIU_MAX_ASSIGN(LCII_pcounters[LCIU_get_thread_id()]
                        .progress_useful_call_consecutive_max,
                    device->did_work_consecutive);
    LCII_pcounters[LCIU_get_thread_id()].progress_useful_call_consecutive_sum +=
        device->did_work_consecutive;
    device->did_work_consecutive = 0;
  }
#endif
  return ret;
}
