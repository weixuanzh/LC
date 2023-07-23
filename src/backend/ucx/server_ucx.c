#include "runtime/lcii.h"
#include "backend/ucx/server_ucx.h"

static int g_endpoint_num = 0;

void LCISD_server_init(LCI_device_t device, LCIS_server_t* s)
{
    LCISI_server_t* server = LCIU_malloc(sizeof(LCISI_server_t));
    *s = (LCIS_server_t)server;
    server->device = device;

    // Create server (ucp_context)
    ucs_status_t status;
    ucp_config_t* config;
    status = ucp_config_read(NULL, NULL, &config);
    ucp_params_t params;
    params.field_mask = UCP_PARAM_FIELD_FEATURES;
    params.features = UCP_FEATURE_TAG;
    ucp_context_h context;
    //printf("ucp_error:%s\n", ucs_status_string(status));
    status = ucp_init(&params, config, &context);
    ucp_context_print_info(context, stdout);
    server->context = context;
    server->endpoint_count = 0;
    
}

void LCISD_server_fina(LCIS_server_t s)
{
  LCISI_server_t* server = (LCISI_server_t*)s;
  LCM_Assert(server->endpoint_count == 0, "Endpoint count is not zero (%d)\n",
             server->endpoint_count);
  ucp_cleanup(server->context);
  free(s);
}

void LCISD_endpoint_init(LCIS_server_t server_pp, LCIS_endpoint_t* endpoint_pp,
                         bool single_threaded)
{
    int endpoint_id = g_endpoint_num++;
    LCISI_endpoint_t* endpoint_p = LCIU_malloc(sizeof(LCISI_endpoint_t));
    *endpoint_pp = (LCIS_endpoint_t)endpoint_p;
    endpoint_p->server = (LCISI_server_t*)server_pp;
    endpoint_p->server->endpoints[endpoint_p->server->endpoint_count++] = endpoint_p;

    // Create endpoint (ucp_worker)
    ucp_worker_h worker;
    ucp_worker_params_t params;
    ucs_status_t status;
    params.field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
    if (single_threaded) {
        params.thread_mode = UCS_THREAD_MODE_SINGLE;
    } else {
        params.thread_mode = UCS_THREAD_MODE_MULTI;
    }

    status = ucp_worker_create(endpoint_p->server->context, &params, &worker);
    //printf("%s\n", ucs_status_string(status));
    LCM_Assert(status == UCS_OK, "Error in creating UCP worker!");
    endpoint_p->worker = worker;

    // Create completion queue
    LCM_dq_init(&endpoint_p->completed_ops, 2 * LCI_CQ_MAX_POLL);
    
    // Set handler for active message (for putImm and putsImm)
    ucp_am_handler_param_t am_params;
    am_params.field_mask = UCP_AM_HANDLER_PARAM_FIELD_CB |
                           UCP_AM_HANDLER_PARAM_FIELD_ID |
                           UCP_AM_HANDLER_PARAM_FIELD_ARG;
    am_params.flags = CQ_AM_ID;
    am_params.cb = am_rma_handler;
    am_params.arg = endpoint_pp;
    ucp_worker_set_am_recv_handler(worker, &am_params);

    // Exchange endpoint address
    endpoint_p->peers = LCIU_malloc(sizeof(ucp_ep_h) * LCI_NUM_PROCESSES);
    ucp_address_t* my_addrs;
    size_t addrs_length;
    status = ucp_worker_get_address(worker, &my_addrs, &addrs_length);
    LCM_Assert(status == UCS_OK, "Error in getting worker address!");
    // Publish worker address
    // Initialize buffers to store address key and address value

    // See ofi endpoint init
    char key[LCM_PMI_STRING_LIMIT + 1];
    char value[LCM_PMI_STRING_LIMIT + 1];
    memset(value, 0, LCM_PMI_STRING_LIMIT + 1);
    memset(key, 0, LCM_PMI_STRING_LIMIT + 1);
    // Set key and value buffers
    sprintf(key, "LCI_KEY_%d_%d", endpoint_id, LCI_RANK);
    memcpy(value, my_addrs, addrs_length);
    lcm_pm_publish(key, value);
    // Publish worker address
    // Initialize buffers to store address length key and value
    char size_key[LCM_PMI_STRING_LIMIT + 1];
    char size_value[LCM_PMI_STRING_LIMIT + 1];
    memset(size_key, 0, LCM_PMI_STRING_LIMIT + 1);
    memset(size_value, 0, LCM_PMI_STRING_LIMIT + 1);
    // Set buffers for address length key and value
    sprintf(size_key, "LCI_SIZE_%d_%d", endpoint_id, LCI_RANK);
    memcpy(size_value, &addrs_length, sizeof(size_t));
    lcm_pm_publish(size_key, size_value);
    lcm_pm_barrier();
    // Receive peer address
    for (int i = 0; i < LCI_NUM_PROCESSES; i++) {
        size_t size;
        // Create ucp endpoint to connect workers
        ucp_ep_params_t ep_params;
        ep_params.field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS |
                               UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE |
                               UCP_EP_PARAM_FIELD_ERR_HANDLER |
                               UCP_EP_PARAM_FIELD_USER_DATA;
        ep_params.err_mode = UCP_ERR_HANDLING_MODE_PEER;
        ep_params.err_handler.cb  = failure_handler;
        ep_params.err_handler.arg = NULL;
        ep_params.user_data = NULL;
        // Receive information (address) required to create ucp endpoint
        if (i != LCI_RANK) {
            // Get length of the specific ucp address
            // Reuse size_key and size_value buffers, reset to 0
            memset(size_value, 0, LCM_PMI_STRING_LIMIT + 1);
            memset(size_key, 0, LCM_PMI_STRING_LIMIT + 1);
            // Set to the correct size key
            sprintf(size_key, "LCI_SIZE_%d_%d", endpoint_id, i);
            // Get length of address, copy to variable "size"
            lcm_pm_getname(i, size_key, size_value);
            memcpy(&size, size_value, sizeof(size_t));
            // Get the specific ucp address
            // Reuse key and value buffers, reset to 0 
            memset(key, 0, LCM_PMI_STRING_LIMIT + 1);
            memset(value, 0, LCM_PMI_STRING_LIMIT + 1);
            // Set the correct address key
            sprintf(key, "LCI_KEY_%d_%d", endpoint_id, i);
            // Get ucp address, copy to variable peer_addr
            lcm_pm_getname(i, key, value);
            char peer_addr[LCM_PMI_STRING_LIMIT + 1];
            memcpy(peer_addr, value, LCM_PMI_STRING_LIMIT + 1);
            // Set peer address
            ep_params.address = (ucp_address_t*) peer_addr;
        } else {
            ep_params.address = my_addrs;
        }
        ucp_ep_h peer;
        ucs_status_t status1;
        status1 = ucp_ep_create(worker, &ep_params, &peer);
        LCM_Assert(status1 == UCS_OK, "Error in creating peer endpoints!");
        (endpoint_p->peers)[i] = peer;
    }
    printf("endpoint %d in rank %d has been created", endpoint_id, LCI_RANK);
    fflush(stdout);
    lcm_pm_barrier();

}

void LCISD_endpoint_fina(LCIS_endpoint_t endpoint_pp)
{
  lcm_pm_barrier();
  LCISI_endpoint_t* endpoint_p = (LCISI_endpoint_t*)endpoint_pp;
  int my_idx = --endpoint_p->server->endpoint_count;
  LCM_Assert(endpoint_p->server->endpoints[my_idx] == endpoint_p,
             "This is not me!\n");
  endpoint_p->server->endpoints[my_idx] = NULL;
  for (int i = 0; i < LCI_NUM_PROCESSES; i++) {
    ucs_status_ptr_t status;
    ucp_request_param_t params;
    params.flags = UCP_EP_CLOSE_FLAG_FORCE;
    status = ucp_ep_close_nbx((endpoint_p->peers)[i], &params);
  }

  // Should other ucp ep owned by other workers be destoryed?
  ucp_worker_destroy(endpoint_p->worker);
  LCM_dq_finalize(&(endpoint_p->completed_ops));
  free(endpoint_pp);
}