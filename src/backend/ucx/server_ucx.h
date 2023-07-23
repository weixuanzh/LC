#ifndef SERVER_UCX_H_
#define SERVER_UCX_H_

#include <ucp/api/ucp.h>

#define CQ_AM_ID 1234
#define CQ_RECV_ID 5678
#define COMMON_TAG 1145
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

typedef struct __attribute__((aligned(LCI_CACHE_LINE))) CQ_wrapper {
  enum LCIS_opcode_t op;
  int rank;
  uint32_t imm_data;
  size_t length;
  void* ep; // ucp endpoint associated with the operation
  void* ctx; // either LCII_packet or LCII_context passed in operations
} CQ_wrapper;

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
} LCISI_endpoint_t;

// Callback function
// Add a entry to completion queue
static void common_handler(void *request, ucs_status_t status, void *ctx) {
  printf("\nCOmmon handler is called!");
  CQ_wrapper* cq_entry = (CQ_wrapper*) ctx;
  LCISI_endpoint_t* ep = (LCISI_endpoint_t*) cq_entry->ep;
  LCM_dq_push_top(&(ep->completed_ops), ctx);
}

static void common_handler_recv(void *request, ucs_status_t status, const ucp_tag_recv_info_t* info, void *ctx) {
  common_handler(NULL, status, ctx);
}

// Invoked after send is completed, sends LCIS_meta to remote CQ
static void send_handler(void* request, ucs_status_t status, void* ctx) {
  // Add entry to completion queue
  printf("Send callback is called!\n");
  fflush(stdout);
  CQ_wrapper* cq_entry = (CQ_wrapper*) ctx;
  LCISI_endpoint_t* ep = (LCISI_endpoint_t*) cq_entry->ep;
  LCM_dq_push_top(&(ep->completed_ops), cq_entry);

  // Send LCI_meta to remote completion queue with active message
  ucs_status_ptr_t send_request;
  ucp_request_param_t send_param;
  // int header = LCI_RANK;
  send_request = ucp_tag_send_nbx(ep->peers[cq_entry->rank], &(cq_entry->imm_data), sizeof(LCIS_meta_t), CQ_RECV_ID, &send_param);
  printf("\n\n\nUCS error in send_handler: %s\n\n\n", ucs_status_string(UCS_PTR_STATUS(send_request)));
  // request = ucp_am_send_nbx(cq_entry->ep, CQ_RECV_ID, &header, sizeof(int), &(cq_entry->imm_data), sizeof(LCIS_meta_t), &send_param);
  LCM_Assert(!UCS_PTR_IS_ERR(send_request), "Error in sending LCIS_meta!");
}

// Struct to use when passing arguments to recv handler
typedef struct __attribute__((aligned(LCI_CACHE_LINE))) recv_args {
  // CQ_entry associated with the operation
  void* ctx;
  // Size of the received message
  size_t size;
  // Buffer to put received message
  void* buf;
  // Buffer used in post_recv
  void* recv_buf;
} recv_args;

// Not useful if LCIS_meta is sent with active message
static void recv_handler(void* request, ucs_status_t status, const ucp_tag_recv_info_t *tag_info, void* ctx) {
  printf("I am called! (recv handler)\n");
  fflush(stdout);
  recv_args* r_args = (recv_args*) ctx;
  CQ_wrapper* cq_entry = (CQ_wrapper*) (r_args->ctx);
  LCISI_endpoint_t* ep = (LCISI_endpoint_t*) (cq_entry->ep);

  // // Receive LCI_meta from sender
  // ucs_status_ptr_t recv_request;
  // // Write LCI_meta in callback function
  // ucp_request_param_t recv_param;
  // recv_param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
  //                           UCP_OP_ATTR_FIELD_USER_DATA;
  // // wrapper is added to the CQ after LCI_meta is delievered (after the 2nd recv)
  // recv_param.cb.recv = common_handler_recv;
  // recv_param.user_data = ctx;
  // recv_request = ucp_tag_recv_nbx(ep->worker, &(cq_entry->imm_data), sizeof(LCIS_meta_t), CQ_RECV_ID, 1, &recv_param);
  // LCM_Assert(!UCS_PTR_IS_ERR(recv_request), "Error in recving LCIS_meta!");
  // Use callback in case operation is completed immediately
  // if (recv_request == UCS_OK) {
  //   common_handler(NULL, status, ctx);
  // }

  // Copy received message and LCIS_meta to correct locations
  char* tmp_buf = (char*) r_args->recv_buf;
  // Last bytes of received data are source rank
  char* addr_start = tmp_buf + tag_info->length - sizeof(int);
  memcpy(&(cq_entry->rank), addr_start, sizeof(int));
  // Middle bytes are LCIS_meta
  addr_start = addr_start - sizeof(LCIS_meta_t);
  memcpy(&(cq_entry->imm_data), addr_start, sizeof(LCIS_meta_t));
  // Front bytes are actual data
  memcpy(r_args->buf, tmp_buf, tag_info->length - sizeof(LCIS_meta_t) - sizeof(int));

  // Add entry to CQ
  ucs_status_t unused;
  printf("\nReceived LCIS_meta: %d", cq_entry->imm_data);
  common_handler(NULL, unused, cq_entry);
  //free(r_args);
}

static void free_buffer(void* request, ucs_status_t status, void* buf) {
  free(buf);
}

// TODO: figure out how to force the send to use eager protocol
// Add entry to local CQ and send LCIS_meta to remote CQ
// Use ucp_am_send so that there is no need to pre-post recv request
static void put_handler(void* request, ucs_status_t status, void* ctx) {
  // Add entry to completion queue
  common_handler(NULL, status, ctx);
  CQ_wrapper* cq_entry = (CQ_wrapper*) ctx;
  LCISI_endpoint_t* ep = (LCISI_endpoint_t*) cq_entry->ep;
  // Send data to remote using active message
  // No callback is needed
  ucs_status_ptr_t put_request;

  // Allocate onto heap, pack rank and LCIS_meta into header buffer
  char* header = malloc(sizeof(int) + sizeof(uint32_t));
  memcpy(header, &LCI_RANK, sizeof(int));
  memcpy(header + sizeof(int), &(cq_entry->imm_data), sizeof(uint32_t));
  ucp_request_param_t params;
  params.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                        UCP_OP_ATTR_FIELD_USER_DATA;
  params.cb.send = free_buffer;
  params.user_data = (void*) header;
  put_request = ucp_am_send_nbx(ep->peers[cq_entry->rank], CQ_AM_ID, header, sizeof(header), NULL, 0, &params);
  LCM_Assert(!UCS_PTR_IS_ERR(put_request), "Error in sending LCIS_meta during rma!");
}

// Specify this when creating ucp workers
// Callback function to update the CQ once an active message has arrived
// The only source of AM is the remote CQ signal after put completion
static ucs_status_t am_rma_handler(void* arg, const void* header, size_t header_length, void* data, size_t length, const ucp_am_recv_param_t* param) {
  char* tmp = (char*) header;
  LCISI_endpoint_t* ep = (LCISI_endpoint_t*) arg;
  CQ_wrapper* cq_entry = malloc(sizeof(CQ_wrapper));
  cq_entry->ep = ep;
  cq_entry->op = LCII_OP_RDMA_WRITE;
  memcpy(&(cq_entry->rank), tmp, sizeof(int));
  memcpy(&(cq_entry->imm_data), tmp + sizeof(int), sizeof(LCIS_meta_t));
  cq_entry->length = length;
  cq_entry->ctx = NULL;
  LCM_dq_push_top(&(ep->completed_ops), cq_entry);
  return UCS_OK;
}

// // Currently unused
// // Same as rma handler except the operation type of the CQ entry is different
// static void am_recv_handler(void* arg, const void* header, size_t header_length, void* data, size_t length, const ucp_am_recv_param_t* param) {
//   LCISI_endpoint_t* ep = (LCISI_endpoint_t*) arg;
//   CQ_wrapper* cq_entry = malloc(sizeof(CQ_wrapper));
//   cq_entry->ep = ep;
//   cq_entry->op = LCII_OP_RECV;
//   cq_entry->imm_data = *((LCIS_meta_t*) data);
//   cq_entry->length = length;
//   cq_entry->rank = *((int*) header);
//   //cq_entry->ctx = NULL;
//   LCM_dq_push_top(&(ep->completed_ops), cq_entry);
// }

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
  status = ucp_mem_map(server->context, &params, &memh);
  if (status != UCS_OK) {
    LCM_Assert(false, "Error in server deregistration!");
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
    LCM_Assert(false, "Error in server deregistration!");
  }
  free(wrapper);
}

static inline LCIS_rkey_t LCISD_rma_rkey(LCIS_mr_t mr)
{ 
  void* packed_addr;
  size_t packed_size;
  ucs_status_t status;
  memh_wrapper* wrapper = (memh_wrapper*) mr.mr_p;
  ucp_memh_pack_params_t params;
  status = ucp_memh_pack(wrapper->memh, &params, &packed_addr, &packed_size);
  LCM_Assert(!UCS_PTR_IS_ERR(status), "Error in packing rkey!");
  LCM_Assert(packed_size <= sizeof(LCIS_rkey_t), "Size exceeds limit!");
  LCIS_rkey_t res;
  memcpy(&res, packed_addr, packed_size);
  return (LCIS_rkey_t) res;
}

// Not necessary if serve send/recv are completed in callback functions
static inline int LCISD_poll_cq(LCIS_endpoint_t endpoint_pp,
                                LCIS_cq_entry_t* entry)
{
  LCISI_endpoint_t* endpoint_p = (LCISI_endpoint_t*)endpoint_pp;
  ucp_worker_progress(endpoint_p->worker);

  int completed_ops = 0;
  for (int i = 0; i < LCM_dq_size(endpoint_p->completed_ops); i++) {
    CQ_wrapper* ep_entry = (CQ_wrapper*) LCM_dq_pop_bot(&(endpoint_p->completed_ops));
    entry[i].ctx = ep_entry->ctx;
    entry[i].imm_data = ep_entry->imm_data;
    entry[i].length = ep_entry->length;
    entry[i].opcode = ep_entry->op;
    entry[i].rank = ep_entry->rank;
    completed_ops++;
    free(ep_entry);
  }
  return completed_ops;
}


static inline void LCISD_post_recv(LCIS_endpoint_t endpoint_pp, void* buf,
                                   uint32_t size, LCIS_mr_t mr, void* ctx)
{

  LCISI_endpoint_t* endpoint_p = (LCISI_endpoint_t*)endpoint_pp;
  ucp_request_param_t recv_param;
  ucs_status_ptr_t request;
  ucp_tag_t tag = LCI_RANK;

  recv_param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                            UCP_OP_ATTR_FIELD_MEMORY_TYPE |
                            UCP_OP_ATTR_FIELD_USER_DATA |
                            UCP_OP_ATTR_FIELD_MEMH;
  recv_param.cb.recv = recv_handler;
  recv_param.memh = mr.mr_p;
  CQ_wrapper* tmp = malloc(sizeof(CQ_wrapper));
  // ep is the UCP_endpoint used in the second recv
  // No need to set imm_data of the wrapper, it is expected to arrive in the second recv
  tmp->ep = (void*) endpoint_p;
  tmp->length = size;
  tmp->op = LCII_OP_RECV;
  tmp->rank = LCI_RANK;
  tmp->ctx = ctx;
  recv_param.memory_type = UCS_MEMORY_TYPE_HOST;

  recv_args* r_args = malloc(sizeof(recv_args));

  // Allocate new buffer to receive packed message (message + LCIS_meta)
  char* newBuffer = malloc(size + sizeof(LCIS_meta_t) + sizeof(int));
  // Pass arguments to recv callback function
  r_args->ctx = tmp;
  r_args->buf = buf;
  r_args->recv_buf = newBuffer;
  r_args->size = size;
  recv_param.user_data = r_args;

  request = ucp_tag_recv_nbx(endpoint_p->worker, newBuffer, size + sizeof(LCIS_meta_t) + sizeof(int), COMMON_TAG, 0, &recv_param);
  LCM_Assert(!UCS_PTR_IS_ERR(request), "Error in recving message!");
  // Use callback in case operation is completed immediately
  if (request == UCS_OK) {
    ucs_status_t unused;
    recv_handler(NULL, unused, NULL, r_args);
  }
  
}

// TODO: figure out how to handle messages of differet sizes
static inline LCI_error_t LCISD_post_sends(LCIS_endpoint_t endpoint_pp,
                                           int rank, void* buf, size_t size,
                                           LCIS_meta_t meta)
{
  printf("I am called!: test message %ld\n", ~((uint64_t) 0));
  fflush(stdout);
  // printf("\nSending LCIS_meta: %d", meta);
  LCISI_endpoint_t* endpoint_p = (LCISI_endpoint_t*)endpoint_pp;
  ucp_request_param_t send_param;
  ucs_status_ptr_t request;
  ucp_tag_t tag = rank;  

  send_param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                            UCP_OP_ATTR_FIELD_MEMORY_TYPE |
                            UCP_OP_ATTR_FIELD_USER_DATA;
  send_param.cb.send = common_handler;
  CQ_wrapper* tmp = malloc(sizeof(CQ_wrapper));
  // The ep in the wrapper is used as destination of the second send
  tmp->ep = (void*) endpoint_p;
  tmp->imm_data = meta;
  tmp->length = size;
  tmp->op = LCII_OP_SEND;
  tmp->rank = rank;
  tmp->ctx = NULL;
  send_param.user_data = tmp;
  send_param.memory_type = UCS_MEMORY_TYPE_HOST;
  // Pack LCIS_meta and data to send together
  char* newBuffer = malloc(size + sizeof(LCIS_meta_t) + sizeof(int));
  // Copy data from buffer to packed buffer
  memcpy(newBuffer, buf, size);
  // Copy LCIS_meta to packed buffer
  memcpy(newBuffer + size, &meta, sizeof(LCIS_meta_t));
  // Copy source rank to packed buffer
  int tmpRank = LCI_RANK;
  memcpy(newBuffer + size + sizeof(LCIS_meta_t), &tmpRank, sizeof(int));
  request = ucp_tag_send_nbx(endpoint_p->peers[rank], newBuffer, size + sizeof(LCIS_meta_t) + sizeof(int), COMMON_TAG, &send_param);
  LCM_Assert(!UCS_PTR_IS_ERR(request), "Error in sending message!");
  // Use callback in case operation is completed immediately
  if (request == UCS_OK) {
    ucs_status_t unused;
    common_handler(NULL, unused, tmp);
  }
  ucp_ep_print_info(endpoint_p->peers[rank], stdout);

  return LCI_OK;
}

// TODO: figure out what LCIS_mr_t is used for
static inline LCI_error_t LCISD_post_send(LCIS_endpoint_t endpoint_pp, int rank,
                                          void* buf, size_t size, LCIS_mr_t mr,
                                          LCIS_meta_t meta, void* ctx)
{
  LCISI_endpoint_t* endpoint_p = (LCISI_endpoint_t*)endpoint_pp;
  ucp_request_param_t send_param;
  ucs_status_ptr_t request;
  ucp_tag_t tag = rank;

  send_param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                            UCP_OP_ATTR_FIELD_MEMORY_TYPE |
                            UCP_OP_ATTR_FIELD_USER_DATA |
                            UCP_OP_ATTR_FIELD_MEMH;
  send_param.cb.send = send_handler;
  CQ_wrapper* tmp = malloc(sizeof(CQ_wrapper));
  tmp->ep = (void*) (endpoint_p->peers[rank]);
  tmp->imm_data = meta;
  tmp->length = size;
  tmp->op = LCII_OP_SEND;
  tmp->rank = rank;
  tmp->ctx = ctx;
  send_param.memh = ((memh_wrapper*) mr.mr_p)->memh;
  send_param.user_data = tmp;
  send_param.memory_type = UCS_MEMORY_TYPE_UNKNOWN;
  request = ucp_tag_send_nbx(endpoint_p->peers[rank], buf, size, tag, &send_param);
  LCM_Assert(!UCS_PTR_IS_ERR(request), "Error in sending message!");
  // Use callback in case operation is completed immediately
  if (request == UCS_OK) {
    ucs_status_t unused;
    send_handler(NULL, unused, tmp);
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
  ucp_request_param_t put_param;
  status = ucp_ep_rkey_unpack(endpoint_p->peers[rank], (void*) (&rkey), &rkey_ptr);
  LCM_Assert(status == UCS_OK, "Error in unpacking RMA key!");


  // Use callback functino to signal completion of put operation
  put_param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                            UCP_OP_ATTR_FIELD_MEMORY_TYPE |
                            UCP_OP_ATTR_FIELD_USER_DATA;
  put_param.cb.send = common_handler;
  CQ_wrapper* tmp = malloc(sizeof(CQ_wrapper));
  tmp->ep = (void*) endpoint_p;
  tmp->length = size;
  tmp->op = LCII_OP_RDMA_WRITE;
  tmp->rank = rank;
  tmp->ctx = NULL;
  put_param.user_data = tmp;
  put_param.memory_type = UCS_MEMORY_TYPE_UNKNOWN;

  uint64_t remote_addr = base + offset;
  ucs_status_ptr_t request;
  request = ucp_put_nbx(endpoint_p->peers[rank], buf, size, remote_addr, rkey_ptr, &put_param);
  LCM_Assert(!UCS_PTR_IS_ERR(request), "Error in RMA puts operation!");

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
  ucp_request_param_t put_param;
  status = ucp_ep_rkey_unpack(endpoint_p->peers[rank], (void*) (&rkey), &rkey_ptr);
  LCM_Assert(status == UCS_OK, "Error in unpacking RMA key!");

  // Create local CQ entry and add it to local CQ
  CQ_wrapper* tmp = malloc(sizeof(CQ_wrapper));
  tmp->ep = (void*) endpoint_p;
  tmp->length = size;
  tmp->op = LCII_OP_RDMA_WRITE;
  tmp->rank = rank;
  tmp->ctx = NULL;
  // RMA write
  put_param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                            UCP_OP_ATTR_FIELD_MEMORY_TYPE |
                            UCP_OP_ATTR_FIELD_USER_DATA;
  put_param.cb.send = common_handler;
  put_param.user_data = tmp;
  put_param.memory_type = UCS_MEMORY_TYPE_UNKNOWN;
  uint64_t remote_addr = base + offset;
  ucs_status_ptr_t request;
  request = ucp_put_nbx(endpoint_p->peers[rank], buf, size, remote_addr, rkey_ptr, &put_param);
  LCM_Assert(!UCS_PTR_IS_ERR(request), "Error in RMA puts operation!");

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
  uintptr_t addr;
  // Unpack the packed rkey
  ucp_rkey_h rkey_ptr;
  ucs_status_t status;
  ucp_request_param_t put_param;
  status = ucp_ep_rkey_unpack(endpoint_p->peers[rank], (void*) (&rkey), &rkey_ptr);
  LCM_Assert(status == UCS_OK, "Error in unpacking RMA key!");

  // Create local CQ entry and add it to local CQ
  CQ_wrapper* tmp = malloc(sizeof(CQ_wrapper));
  tmp->ep = (void*) endpoint_p;
  tmp->length = size;
  tmp->op = LCII_OP_RDMA_WRITE;
  tmp->rank = rank;
  tmp->ctx = NULL;
  tmp->imm_data = meta;
  // RMA write
  put_param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                            UCP_OP_ATTR_FIELD_MEMORY_TYPE |
                            UCP_OP_ATTR_FIELD_USER_DATA;
  put_param.cb.send = put_handler;
  put_param.user_data = tmp;
  put_param.memory_type = UCS_MEMORY_TYPE_UNKNOWN;
  uint64_t remote_addr = base + offset;
  ucs_status_ptr_t request;
  request = ucp_put_nbx(endpoint_p->peers[rank], buf, size, remote_addr, rkey_ptr, &put_param);
  LCM_Assert(!UCS_PTR_IS_ERR(request), "Error in RMA Immputs operation!");

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
  uintptr_t addr;
  // Unpack the packed rkey
  ucp_rkey_h rkey_ptr;
  ucs_status_t status;
  ucp_request_param_t put_param;
  status = ucp_ep_rkey_unpack(endpoint_p->peers[rank], (void*) (&rkey), &rkey_ptr);
  LCM_Assert(status == UCS_OK, "Error in unpacking RMA key!");

  // Create local CQ entry and add it to local CQ
  CQ_wrapper* tmp = malloc(sizeof(CQ_wrapper));
  tmp->ep = (void*) endpoint_p;
  tmp->length = size;
  tmp->op = LCII_OP_RDMA_WRITE;
  tmp->rank = rank;
  tmp->ctx = ctx;
  tmp->imm_data = meta;
  // RMA write
  put_param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                            UCP_OP_ATTR_FIELD_MEMORY_TYPE |
                            UCP_OP_ATTR_FIELD_USER_DATA;
  put_param.cb.send = put_handler;
  put_param.user_data = tmp;
  put_param.memory_type = UCS_MEMORY_TYPE_UNKNOWN;
  uint64_t remote_addr = base + offset;
  ucs_status_ptr_t request;
  request = ucp_put_nbx(endpoint_p->peers[rank], buf, size, remote_addr, rkey_ptr, &put_param);
  LCM_Assert(!UCS_PTR_IS_ERR(request), "Error in RMA Immputs operation!");

  return LCI_OK;
  
}

#endif