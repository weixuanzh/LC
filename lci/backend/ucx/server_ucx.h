#ifndef SERVER_UCX_H_
#define SERVER_UCX_H_

#include <ucp/api/ucp.h>

#define CQ_AM_ID 1234
#define CQ_RECV_ID 5678
#define COMMON_TAG 1145
#define CQ_LENGTH 8192
struct LCISI_endpoint_t;

typedef enum {
  UCP_EMPTY = 0,
  UCP_SEND,
  UCP_RECV,
  UCP_PUT,
} operations;

typedef struct __attribute__((aligned(LCI_CACHE_LINE))) memh_wrapper {
  ucp_mem_h memh;
  ucp_context_h context;
} memh_wrapper;

typedef struct __attribute__((aligned(LCI_CACHE_LINE))) LCISI_cq_entry {
  enum LCIS_opcode_t op;
  int rank;
  uint32_t imm_data;
  size_t length;
  void* ep; // ucp endpoint associated with the operation
  void* ctx; // either LCII_packet or LCII_context passed in operations
} LCISI_cq_entry;

typedef struct __attribute__((aligned(LCI_CACHE_LINE))) LCISI_server_t {
  LCI_device_t device;
  struct LCISI_endpoint_t* endpoints[LCI_SERVER_MAX_ENDPOINTS];
  int endpoint_count;
  ucp_context_h context;
} LCISI_server_t;

typedef struct __attribute__((aligned(LCI_CACHE_LINE))) LCISI_endpoint_t {
  LCISI_server_t* server;
  ucp_worker_h worker;
  ucp_ep_h* peers;
  size_t* addrs_length;
  LCM_dequeue_t completed_ops;
  LCIU_spinlock_t lock;
} LCISI_endpoint_t;

// pack meta (4 bytes) and rank (4 bytes) into ucp_tag_t (8 bytes)
// meta | rank
static ucp_tag_t create_tag(LCIS_meta_t meta, int rank) {
  ucp_tag_t tag;
  memcpy(&tag, &meta, sizeof(LCIS_meta_t));
  memcpy((char*)(&tag) + sizeof(LCIS_meta_t), &rank, sizeof(int));
  return tag;
}

// unpack meta and rank stored in ucp_tag_t
static void unpack_tag(ucp_tag_t tag, LCIS_meta_t* meta_ptr, int* int_ptr) {
  memcpy(meta_ptr, &tag, sizeof(LCIS_meta_t));
  memcpy(int_ptr, (char*)(&tag) + sizeof(LCIS_meta_t), sizeof(int));
}

// Add a entry to completion queue
static void push_cq(void* entry) {
  LCISI_cq_entry* cq_entry = (LCISI_cq_entry*) entry;
  LCISI_endpoint_t* ep = (LCISI_endpoint_t*) cq_entry->ep;

  LCIU_acquire_spinlock(&(ep->lock));
  int status = LCM_dq_push_top(&(ep->completed_ops), entry);
  LCI_Assert(status != LCM_RETRY, "Too many entries in CQ!");
  LCIU_release_spinlock(&(ep->lock));
}

// Struct to use when passing arguments to recv handler
typedef struct __attribute__((aligned(LCI_CACHE_LINE))) LCISI_cb_args {
  // CQ_entry associated with the operation
  void* entry;
  // Size of the message to send/receive
  size_t size;
  // User provided buffer address
  void* buf;
  // Buffer to store packed message
  void* packed_buf;
} LCISI_cb_args;

// Called when ucp receives a message
// Unpack received data, update completion queue, free allocated buffers
static void recv_handler(void* request, ucs_status_t status, const ucp_tag_recv_info_t *tag_info, void* args) {
  LCISI_cb_args* cb_args = (LCISI_cb_args*) args;
  LCISI_cq_entry* cq_entry = (LCISI_cq_entry*) (cb_args->entry);
  LCISI_endpoint_t* ep = (LCISI_endpoint_t*) (cq_entry->ep);
  
  int msg_length = 0;

  // Check if user provided buffer size is enough to receive message
  LCI_Assert(cb_args->size >= tag_info->length, "Message size greater than allocated buffer!");
  // Check if received message length makes sense (cannot be too short)
  LCI_Assert(tag_info->length >= 0, "Message length is too short to be valid!");
  cq_entry->length = tag_info->length;

  if (cq_entry->length != 0) {
    // Nonzero message length indicates completion of recv operation
    cq_entry->op = LCII_OP_RECV;
    unpack_tag(tag_info->sender_tag, &(cq_entry->imm_data), &(cq_entry->rank));
  } else {
    // Zero message length indicates the completion of RDMA operation
    cq_entry->op = LCII_OP_RDMA_WRITE;
    unpack_tag(tag_info->sender_tag, &(cq_entry->imm_data), &(cq_entry->rank));
  }

  // Add entry to CQ
  ucs_status_t unused;
  push_cq(cq_entry);

  // Free resources
  free(cb_args->packed_buf);
  free(cb_args);
  if (request != NULL) {
    ucp_request_free(request);
  }
}

// Invoked after send is completed
// Free allocated buffer, update completion queue
static void send_handler(void* request, ucs_status_t status, void* args) {
  LCISI_cb_args* cb_args = (LCISI_cb_args*) args;

  // Add entry to completion queue
  if (cb_args->entry != NULL) {
    push_cq(cb_args->entry);
  }

  // Free packed buffer used in ucp send
  if (cb_args->packed_buf != NULL) {
    free(cb_args->packed_buf);
  }
  if (request != NULL) {
    ucp_request_free(request);
  }
  free(cb_args);
}

// Add entry to local completion queue, send LCIS_meta and source rank to remote CQ
static void put_handler(void* request, ucs_status_t status, void* args) {

  LCISI_cb_args* cb_args = (LCISI_cb_args*) args;
  LCISI_cq_entry* cq_entry = (LCISI_cq_entry*) cb_args->entry;
  LCISI_endpoint_t* ep = (LCISI_endpoint_t*) cq_entry->ep;

  // Add entry to completion queue
  push_cq(cq_entry);

  // Set arguments of am send callback to free allocated resources
  LCISI_cb_args* am_cb_args = malloc(sizeof(LCISI_cb_args));
  am_cb_args->packed_buf = cb_args->packed_buf;
  am_cb_args->buf = NULL;
  am_cb_args->entry = NULL;

  // Send data to remote using active message
  // Data to send is stored in packed_buf member of cb_args (already prepared in post_put function)
  ucs_status_ptr_t put_request;
  ucp_request_param_t params;
  params.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                        UCP_OP_ATTR_FIELD_USER_DATA;
  params.cb.send = send_handler;
  params.user_data = (void*) am_cb_args;
  put_request = ucp_tag_send_nbx(ep->peers[cq_entry->rank], cb_args->packed_buf, 0, *((ucp_tag_t*)(cb_args->packed_buf)), &params);
  LCI_Assert(!UCS_PTR_IS_ERR(put_request), "Error in sending LCIS_meta during rma!");
  // Use callback in case send is completed immediately
  if (put_request == UCS_OK) {
    ucs_status_t unused;
    send_handler(put_request, unused, am_cb_args);
  }
  free(cb_args);
}

static void failure_handler(void *request, ucp_ep_h ep, ucs_status_t status) {
  printf("\nError!");
  printf("%s", ucs_status_string(status));
}

static inline LCIS_mr_t LCISD_rma_reg(LCIS_server_t s, void* buf, size_t size)
{ 
  LCISI_server_t* server = (LCISI_server_t*) s;
  LCIS_mr_t mr;
  ucp_mem_h memh;
  ucp_mem_map_params_t params;
  ucs_status_t status;
  memh_wrapper* wrapper = malloc(sizeof(memh_wrapper));
  
  params.field_mask = UCP_MEM_MAP_PARAM_FIELD_ADDRESS |
                      UCP_MEM_MAP_PARAM_FIELD_LENGTH |
                      UCP_MEM_MAP_PARAM_FIELD_PROT;
  params.address = buf;
  params.length = size;
  params.prot = UCP_MEM_MAP_PROT_REMOTE_WRITE |
                UCP_MEM_MAP_PROT_LOCAL_READ |
                UCP_MEM_MAP_PROT_LOCAL_WRITE;
  //params.exported_memh_buffer = malloc(sizeof(ucp_mem_h));
  status = ucp_mem_map(server->context, &params, &memh);
  if (status != UCS_OK) {
    LCI_Assert(false, "Error in server deregistration!");
  }
  mr.address = buf;
  mr.length = size;
  wrapper->context = server->context;
  wrapper->memh = memh;
  mr.mr_p = wrapper;
  return mr;
}

static inline void LCISD_rma_dereg(LCIS_mr_t mr)
{
  ucs_status_t status;
  memh_wrapper* wrapper = (memh_wrapper*) mr.mr_p;
  status = ucp_mem_unmap(wrapper->context, wrapper->memh);
  if (status != UCS_OK) {
    LCI_Assert(false, "Error in server deregistration!");
  }
  free(wrapper);
}

static inline LCIS_rkey_t LCISD_rma_rkey(LCIS_mr_t mr)
{ 
  void* packed_addr;
  size_t packed_size;
  ucs_status_t status;
  memh_wrapper* wrapper = (memh_wrapper*) mr.mr_p;
  status = ucp_rkey_pack(wrapper->context, wrapper->memh, &packed_addr, &packed_size);
  LCI_Assert(!UCS_PTR_IS_ERR(status), "Error in packing rkey!");
  LCI_Assert(packed_size <= sizeof(LCIS_rkey_t), "Size exceeds limit!");
  LCIS_rkey_t res;
  memset(&res, 0, sizeof(LCIS_rkey_t));
  memcpy(&res, packed_addr, packed_size);
  return res;
}

// 
static inline int LCISD_poll_cq(LCIS_endpoint_t endpoint_pp,
                                LCIS_cq_entry_t* entry)
{
  LCISI_endpoint_t* endpoint_p = (LCISI_endpoint_t*)endpoint_pp;
  ucp_worker_progress(endpoint_p->worker);
  int num_entries = 0;

  #ifdef LCI_ENABLE_MULTITHREAD_PROGRESS
  LCIU_acquire_spinlock(&(endpoint_p->lock));
  #endif
  while (num_entries < LCI_CQ_MAX_POLL && LCM_dq_size(endpoint_p->completed_ops) > 0) {
    LCISI_cq_entry* cq_entry = (LCISI_cq_entry*) LCM_dq_pop_bot(&(endpoint_p->completed_ops));
    entry[num_entries].ctx = cq_entry->ctx;
    entry[num_entries].imm_data = cq_entry->imm_data;
    entry[num_entries].length = cq_entry->length;
    entry[num_entries].opcode = cq_entry->op;
    entry[num_entries].rank = cq_entry->rank;
    num_entries++;
    free(cq_entry);
  }
  #ifdef LCI_ENABLE_MULTITHREAD_PROGRESS
  LCIU_release_spinlock(&(endpoint_p->lock));
  #endif

  return num_entries;
}

static inline LCI_error_t LCISD_post_recv(LCIS_endpoint_t endpoint_pp, void* buf,
                                   uint32_t size, LCIS_mr_t mr, void* ctx)
{

  LCISI_endpoint_t* endpoint_p = (LCISI_endpoint_t*)endpoint_pp;
  ucp_request_param_t recv_param;
  ucs_status_ptr_t request;

  // Prepare CQ entry associated with this operation
  // No need to set imm_data and rank, this is expected to arrive upon receive
  LCISI_cq_entry* cq_entry = malloc(sizeof(LCISI_cq_entry));
  cq_entry->ep = (void*) endpoint_p;
  cq_entry->length = size;
  cq_entry->op = LCII_OP_RECV;
  cq_entry->ctx = ctx;

  // Set argument for recv callback
  LCISI_cb_args* cb_args = malloc(sizeof(LCISI_cb_args));
  cb_args->entry = cq_entry;
  cb_args->buf = buf;
  cb_args->packed_buf = NULL;
  cb_args->size = size;

  // Setup recv parameters
  recv_param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                            UCP_OP_ATTR_FIELD_MEMORY_TYPE |
                            UCP_OP_ATTR_FIELD_USER_DATA |
                            UCP_OP_ATTR_FLAG_NO_IMM_CMPL |
                            UCP_OP_ATTR_FIELD_MEMH;
  recv_param.cb.recv = recv_handler;
  recv_param.memory_type = UCS_MEMORY_TYPE_HOST;
  recv_param.user_data = cb_args;
  recv_param.memh = mr.mr_p;
  
  // Receive message, check for errors
  request = ucp_tag_recv_nbx(endpoint_p->worker, buf, size, COMMON_TAG, 0, &recv_param);
  if (UCS_PTR_IS_ERR(request)) {
    LCI_Assert(!UCS_PTR_IS_ERR(request), "Error in recving message!");
    return LCI_ERR_FATAL;
  }

  return LCI_OK;

}

static inline LCI_error_t LCISD_post_sends(LCIS_endpoint_t endpoint_pp,
                                           int rank, void* buf, size_t size,
                                           LCIS_meta_t meta)
{
  LCISI_endpoint_t* endpoint_p = (LCISI_endpoint_t*)endpoint_pp;
  ucs_status_ptr_t request;

  // Prepare CQ entry associated with this operation
  LCISI_cq_entry* cq_entry = malloc(sizeof(LCISI_cq_entry));
  cq_entry->ep = (void*) endpoint_p;
  cq_entry->length = size;
  cq_entry->op = LCII_OP_SEND;
  cq_entry->rank = rank;
  cq_entry->ctx = NULL;

  // Set argument for send callback
  LCISI_cb_args* cb_args = malloc(sizeof(LCISI_cb_args));
  cb_args->entry = cq_entry;
  cb_args->packed_buf = NULL;
  cb_args->buf = NULL;

  // Setup send parameters
  ucp_request_param_t send_param;
  send_param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                            UCP_OP_ATTR_FIELD_MEMORY_TYPE |
                            UCP_OP_ATTR_FIELD_USER_DATA;
  send_param.cb.send = send_handler;
  send_param.user_data = cb_args;
  send_param.memory_type = UCS_MEMORY_TYPE_HOST;

  // Send message, check for errors
  // LCIS_meta_t and source rank are delievered in ucp tag
  request = ucp_tag_send_nbx(endpoint_p->peers[rank], buf, size, create_tag(meta, LCI_RANK), &send_param);
  if (UCS_PTR_IS_ERR(request)) {
    LCI_Assert(!UCS_PTR_IS_ERR(request), "Error in posting sends!");
    return LCI_ERR_FATAL;
  }

  // Use callback in case operation is completed immediately
  if (request == UCS_OK) {
    ucs_status_t unused;
    send_handler(NULL, unused, cb_args);
  }

  return LCI_OK;
}

static inline LCI_error_t LCISD_post_send(LCIS_endpoint_t endpoint_pp, int rank,
                                          void* buf, size_t size, LCIS_mr_t mr,
                                          LCIS_meta_t meta, void* ctx)
{
  LCISI_endpoint_t* endpoint_p = (LCISI_endpoint_t*)endpoint_pp;
  ucs_status_ptr_t request;

  // Prepare CQ entry associated with this operation
  LCISI_cq_entry* cq_entry = malloc(sizeof(LCISI_cq_entry));
  cq_entry->ep = (void*) (endpoint_p);
  cq_entry->length = size;
  cq_entry->op = LCII_OP_SEND;
  cq_entry->rank = rank;
  cq_entry->ctx = ctx;

  // Set argument for send callback
  LCISI_cb_args* cb_args = malloc(sizeof(LCISI_cb_args));
  cb_args->entry = cq_entry;
  cb_args->packed_buf = NULL;
  cb_args->buf = NULL;

  // Setup send parameters
  ucp_request_param_t send_param;
  send_param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                            UCP_OP_ATTR_FIELD_MEMORY_TYPE |
                            UCP_OP_ATTR_FIELD_USER_DATA |
                            UCP_OP_ATTR_FIELD_MEMH;
  send_param.cb.send = send_handler;
  send_param.user_data = cb_args;
  send_param.memh = ((memh_wrapper*) mr.mr_p)->memh;
  send_param.memory_type = UCS_MEMORY_TYPE_HOST;
  
  // Send message, check for errors
  request = ucp_tag_send_nbx(endpoint_p->peers[rank], buf, size, create_tag(meta, LCI_RANK), &send_param);
  if (UCS_PTR_IS_ERR(request)) {
    LCI_Assert(!UCS_PTR_IS_ERR(request), "Error in posting send!");
    return LCI_ERR_FATAL;
  }

  // Use callback in case operation is completed immediately
  if (request == UCS_OK) {
    ucs_status_t unused;
    send_handler(NULL, unused, cb_args);
  }
  
  return LCI_OK;
}

// TODO: figure out the difference in handling messages of different sizes
static inline LCI_error_t LCISD_post_puts(LCIS_endpoint_t endpoint_pp, int rank,
                                          void* buf, size_t size,
                                          uintptr_t base, LCIS_offset_t offset,
                                          LCIS_rkey_t rkey)
{
  LCISI_endpoint_t* endpoint_p = (LCISI_endpoint_t*)endpoint_pp;
  
  // Unpack the packed rkey
  ucp_rkey_h rkey_ptr;
  ucs_status_t status;
  void* tmp_rkey = malloc(sizeof(LCIS_rkey_t));
  memset(tmp_rkey, 0, sizeof(LCIS_rkey_t));
  memcpy(tmp_rkey, &rkey, sizeof(LCIS_rkey_t));
  status = ucp_ep_rkey_unpack(endpoint_p->peers[rank], tmp_rkey, &rkey_ptr);
  LCI_Assert(status == UCS_OK, "Error in unpacking RMA key!");

  // Prepare CQ entry associated with this operation
  LCISI_cq_entry* cq_entry = malloc(sizeof(LCISI_cq_entry));
  cq_entry->ep = (void*) endpoint_p;
  cq_entry->length = size;
  cq_entry->op = LCII_OP_SEND;
  cq_entry->rank = rank;
  cq_entry->ctx = NULL;

  // Set argument for send callback
  LCISI_cb_args* cb_args = malloc(sizeof(LCISI_cb_args));
  cb_args->entry = cq_entry;
  cb_args->buf = NULL;
  cb_args->packed_buf = NULL;
  
  // Setup send parameters
  ucp_request_param_t put_param;
  put_param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                            UCP_OP_ATTR_FIELD_MEMORY_TYPE |
                            UCP_OP_ATTR_FIELD_USER_DATA;
  // No need to signal remote completion
  put_param.cb.send = send_handler;
  put_param.user_data = cb_args;
  put_param.memory_type = UCS_MEMORY_TYPE_HOST;

  // Send message, check for errors
  uint64_t remote_addr = base + offset;
  ucs_status_ptr_t request;
  request = ucp_put_nbx(endpoint_p->peers[rank], buf, size, remote_addr, rkey_ptr, &put_param);
  LCI_Assert(!UCS_PTR_IS_ERR(request), "Error in RMA puts operation!");

  // Use callback in case operation is completed immediately
  if (request == UCS_OK) {
    ucs_status_t unused;
    send_handler(NULL, unused, cb_args);
  }

  return LCI_OK;

}

static inline LCI_error_t LCISD_post_put(LCIS_endpoint_t endpoint_pp, int rank,
                                         void* buf, size_t size, LCIS_mr_t mr,
                                         uintptr_t base, LCIS_offset_t offset,
                                         LCIS_rkey_t rkey, void* ctx)
{
  LCISI_endpoint_t* endpoint_p = (LCISI_endpoint_t*)endpoint_pp;
  
  // Unpack the packed rkey
  ucp_rkey_h rkey_ptr;
  ucs_status_t status;
  void* tmp_rkey = malloc(sizeof(LCIS_rkey_t));
  memset(tmp_rkey, 0, sizeof(LCIS_rkey_t));
  memcpy(tmp_rkey, &rkey, sizeof(LCIS_rkey_t));
  status = ucp_ep_rkey_unpack(endpoint_p->peers[rank], tmp_rkey, &rkey_ptr);
  LCI_Assert(status == UCS_OK, "Error in unpacking RMA key!");

  // Prepare CQ entry associated with this operation
  LCISI_cq_entry* cq_entry = malloc(sizeof(LCISI_cq_entry));
  cq_entry->ep = (void*) endpoint_p;
  cq_entry->length = size;
  cq_entry->op = LCII_OP_SEND;
  cq_entry->rank = rank;
  cq_entry->ctx = ctx;

  // Set argument for send callback
  LCISI_cb_args* cb_args = malloc(sizeof(LCISI_cb_args));
  cb_args->entry = cq_entry;
  cb_args->buf = NULL;
  cb_args->packed_buf = NULL;
  
  // Setup send parameters
  ucp_request_param_t put_param;
  put_param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                            UCP_OP_ATTR_FIELD_MEMORY_TYPE |
                            UCP_OP_ATTR_FIELD_USER_DATA;
  // No need to signal remote completion
  put_param.cb.send = send_handler;
  put_param.user_data = cb_args;
  put_param.memory_type = UCS_MEMORY_TYPE_HOST;

  // Send message, check for errors
  uint64_t remote_addr = base + offset;
  ucs_status_ptr_t request;
  request = ucp_put_nbx(endpoint_p->peers[rank], buf, size, remote_addr, rkey_ptr, &put_param);
  LCI_Assert(!UCS_PTR_IS_ERR(request), "Error in RMA puts operation!");

  // Use callback in case operation is completed immediately
  if (request == UCS_OK) {
    ucs_status_t unused;
    send_handler(NULL, unused, cb_args);
  }

  return LCI_OK;

}

// Put and send meta to remote CQ
static inline LCI_error_t LCISD_post_putImms(LCIS_endpoint_t endpoint_pp,
                                             int rank, void* buf, size_t size,
                                             uintptr_t base,
                                             LCIS_offset_t offset,
                                             LCIS_rkey_t rkey, uint32_t meta)
{
  LCISI_endpoint_t* endpoint_p = (LCISI_endpoint_t*)endpoint_pp;
  
  // Unpack the packed rkey
  ucp_rkey_h rkey_ptr;
  ucs_status_t status;
  void* tmp_rkey = malloc(sizeof(LCIS_rkey_t));
  memset(tmp_rkey, 0, sizeof(LCIS_rkey_t));
  memcpy(tmp_rkey, &rkey, sizeof(LCIS_rkey_t));
  status = ucp_ep_rkey_unpack(endpoint_p->peers[rank], tmp_rkey, &rkey_ptr);
  LCI_Assert(status == UCS_OK, "Error in unpacking RMA key!");

  // Prepare CQ entry associated with this operation
  LCISI_cq_entry* cq_entry = malloc(sizeof(LCISI_cq_entry));
  cq_entry->ep = (void*) endpoint_p;
  cq_entry->length = size;
  cq_entry->op = LCII_OP_SEND;
  cq_entry->rank = rank;
  cq_entry->ctx = NULL;

  // Set argument for send callback
  LCISI_cb_args* cb_args = malloc(sizeof(LCISI_cb_args));
  // Stores LCIS_meta and source rank to send to remote completion queue
  ucp_tag_t* packed_buf = malloc(sizeof(ucp_tag_t));
  *packed_buf = create_tag(meta, LCI_RANK);

  cb_args->entry = cq_entry;
  cb_args->buf = NULL;
  cb_args->packed_buf = packed_buf;
  
  // Setup send parameters
  ucp_request_param_t put_param;
  put_param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                            UCP_OP_ATTR_FIELD_MEMORY_TYPE |
                            UCP_OP_ATTR_FIELD_USER_DATA;
  // Deliever data to remote CQ with active message
  put_param.cb.send = put_handler;
  put_param.user_data = cb_args;
  put_param.memory_type = UCS_MEMORY_TYPE_HOST;

  // Send message, check for errors
  uint64_t remote_addr = base + offset;
  ucs_status_ptr_t request;
  request = ucp_put_nbx(endpoint_p->peers[rank], buf, size, remote_addr, rkey_ptr, &put_param);
  LCI_Assert(!UCS_PTR_IS_ERR(request), "Error in RMA puts operation!");

  // Use callback in case operation is completed immediately
  if (request == UCS_OK) {
    ucs_status_t unused;
    put_handler(NULL, unused, cb_args);
  }

  return LCI_OK;

}

static inline LCI_error_t LCISD_post_putImm(LCIS_endpoint_t endpoint_pp,
                                            int rank, void* buf, size_t size,
                                            LCIS_mr_t mr, uintptr_t base,
                                            LCIS_offset_t offset,
                                            LCIS_rkey_t rkey, LCIS_meta_t meta,
                                            void* ctx)
{
  LCISI_endpoint_t* endpoint_p = (LCISI_endpoint_t*)endpoint_pp;
  
  // Unpack the packed rkey
  ucp_rkey_h rkey_ptr;
  ucs_status_t status;
  void* tmp_rkey = malloc(sizeof(LCIS_rkey_t));
  memset(tmp_rkey, 0, sizeof(LCIS_rkey_t));
  memcpy(tmp_rkey, &rkey, sizeof(LCIS_rkey_t));
  status = ucp_ep_rkey_unpack(endpoint_p->peers[rank], tmp_rkey, &rkey_ptr);
  LCI_Assert(status == UCS_OK, "Error in unpacking RMA key!");

  // Prepare CQ entry associated with this operation
  LCISI_cq_entry* cq_entry = malloc(sizeof(LCISI_cq_entry));
  cq_entry->ep = (void*) endpoint_p;
  cq_entry->length = size;
  cq_entry->op = LCII_OP_SEND;
  cq_entry->rank = rank;
  cq_entry->ctx = ctx;

  // Set argument for send callback
  LCISI_cb_args* cb_args = malloc(sizeof(LCISI_cb_args));
  // Stores LCIS_meta and source rank to send to remote completion queue
  ucp_tag_t* packed_buf = malloc(sizeof(ucp_tag_t));
  *(packed_buf) = create_tag(meta, LCI_RANK);

  cb_args->entry = cq_entry;
  cb_args->buf = endpoint_p->peers[rank];
  cb_args->packed_buf = packed_buf;
  
  // Setup send parameters
  ucp_request_param_t put_param;
  put_param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                            UCP_OP_ATTR_FIELD_MEMORY_TYPE |
                            UCP_OP_ATTR_FIELD_USER_DATA;
  // Deliever data to remote CQ with active message
  put_param.cb.send = put_handler;
  put_param.user_data = cb_args;
  put_param.memory_type = UCS_MEMORY_TYPE_HOST;

  // Send message, check for errors
  uint64_t remote_addr = base + offset;
  ucs_status_ptr_t request;
  request = ucp_put_nbx(endpoint_p->peers[rank], buf, size, remote_addr, rkey_ptr, &put_param);
  LCI_Assert(!UCS_PTR_IS_ERR(request), "Error in RMA puts operation!");

  // Use callback in case operation is completed immediately
  if (request == UCS_OK) {
    ucs_status_t unused;
    put_handler(NULL, unused, cb_args);
  }

  return LCI_OK;

}

#endif