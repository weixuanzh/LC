#include "runtime/lcii.h"
#include "backend/ucx/server_ucx.h"

static int g_endpoint_num = 0;

// Assuming that my_addrs is no longer than 256 bytes
// Takes my_addrs and addrs_length as input
// Outputs encoded_addrs and null_locations
// encoded_addrs is simply my_addrs with null removed
// null_locations indicates position of removed null and is used for reconstruction
void encode_ucp_address1(char* my_addrs, int addrs_length, char* encoded_addrs, char* null_locations) {
    char* start = my_addrs;
    int count = 0;
    int loc_idx = 0;
    int encoded_idx = 0;
    for (int i = 0; i < addrs_length; i++) {
        if (my_addrs[i] == '\0') {
            null_locations[loc_idx] = (uint8_t) i;
            loc_idx++;
        } else {
            encoded_addrs[encoded_idx] = my_addrs[i];
            encoded_idx++;
        }
    }
}

// original_length is the length of address before encoding
void decode_ucp_address1(char* encoded_addrs, int original_length, uint8_t* null_locations, char* decoded_addrs) {
    int null_idx = 0;
    int encoded_idx = 0;
    for (int i = 0; i < original_length; i++) {
        if (i == null_locations[null_idx]) {
            decoded_addrs[i] = '\0';
            null_idx++;
        } else {
            decoded_addrs[i] = encoded_addrs[encoded_idx];
            encoded_idx++;
        }
    }
}

// Decodes a ucp_address into a series of hex decimal number
// Delimiters: '-' stands for null in address, ',' indicates that the number continues
// Possible output: 1435ab-52fa234553ba3f12f,3fa-453d1-
void encode_ucp_address(void* my_addrs, int addrs_length, char* encoded_value) {
    int segment_length = 0;
    int segment_offset = 0;
    int value_offset = -1;
    char tmp_dash[1];
    char tmp_comma[1];
    memset(tmp_dash, '-', 1);
    memset(tmp_comma, ',', 1);
    for (int i = 0; i < addrs_length; i++) {
        // Convert a segment of address to int, store it as a string that represents the number
        // A segment is completed when either length is equal to uint64_t or a null is reached
        if (segment_length == sizeof(uint64_t) || ((char*)my_addrs)[i] == '\0') {
            LCM_Assert(segment_length <= sizeof(uint64_t), "ucp address setgment is too long!");
            char padded_addrs[sizeof(uint64_t)];
            uint64_t tmp_num = 0;
            // Add proper padding to interpret addrs as uint64_t
            if (segment_length < sizeof(uint64_t)) {
                memset(padded_addrs, 0, sizeof(uint64_t) - segment_length);
                memcpy(padded_addrs + sizeof(uint64_t) - segment_length, ((char*)my_addrs) + segment_offset, segment_length);
                tmp_num = *((uint64_t*)padded_addrs);
            } else {
                tmp_num = *((uint64_t*)((char*)my_addrs) + segment_offset);
            }
            segment_offset = i + 1;
            segment_length = 0;
            // Interpret data as uint64_t, convert to string in hex decimal
            sprintf((char*)encoded_value + value_offset + 1, "%lx", tmp_num);
            value_offset = strlen(encoded_value);
            // When the segment terminates due to reaching null, append a dash
            // otherwise append a comma
            if (((char*)my_addrs)[i] == '\0') {
                memcpy((char*)encoded_value + value_offset, tmp_dash, 1);
            } else {
                segment_offset = i;
                segment_length = 1;
                memcpy((char*)encoded_value + value_offset, tmp_comma, 1);
            }
        } else {
            segment_length++;
        }
    }
}

void decode_ucp_address(void* encoded_addrs, size_t encoded_length, void* decoded_addrs) {
    int segment_length = 0;
    int value_offset = 0;
    for (int j = 0; j < encoded_length; j++) {
        if (((char*)encoded_addrs)[j] == '-' || ((char*)encoded_addrs)[j] == ',') {
            // Copy one segment of the decoded address and convert
            char seg_buf[segment_length];
            memcpy(seg_buf, encoded_addrs + j - segment_length, segment_length);
            uint64_t tmp_num = (uint64_t) strtol(seg_buf, NULL, 16);
            // Copy nonzero bytes of the converted num (leading 0s are added padding)
            int padding_count = 0;
            // find the first nonzero byte
            while (*((char*)(&tmp_num + padding_count)) == '\0') {
                padding_count++;
            }
            memcpy(decoded_addrs + value_offset, (char*)(&tmp_num) + padding_count, sizeof(uint64_t) - padding_count);
            // Skip one byte if delimiter is dash (represents null in original address), otherwise (comma) don't skip
            value_offset = value_offset + sizeof(uint64_t) - padding_count + 1;
        } else {
            segment_length++;
        }
    }
}

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
    params.features = UCP_FEATURE_TAG |
                      UCP_FEATURE_RMA;
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
    // Currently not useful
    ucp_am_handler_param_t am_params;
    am_params.field_mask = UCP_AM_HANDLER_PARAM_FIELD_CB |
                           UCP_AM_HANDLER_PARAM_FIELD_ID |
                           UCP_AM_HANDLER_PARAM_FIELD_ARG;
    am_params.id = CQ_AM_ID;
    am_params.arg = endpoint_pp;
    ucs_status_t tmp;
    tmp = ucp_worker_set_am_recv_handler(worker, &am_params);
    printf("\n%s", ucs_status_string(tmp));

    // Exchange endpoint address
    endpoint_p->peers = LCIU_malloc(sizeof(ucp_ep_h) * LCI_NUM_PROCESSES);
    ucp_address_t* my_addrs;
    size_t addrs_length;
    status = ucp_worker_get_address(worker, &my_addrs, &addrs_length);
    LCM_Assert(status == UCS_OK, "Error in getting worker address!");

    // Publish worker address
    // Worker address is encoded into a string and an array of index of nulls in the original address
    // Keys to use when publishing address
    char encoded_key[LCM_PMI_STRING_LIMIT + 1];
    char nulls_key[LCM_PMI_STRING_LIMIT + 1];
    char size_key[LCM_PMI_STRING_LIMIT + 1];
    memset(encoded_key, 0, LCM_PMI_STRING_LIMIT + 1);
    memset(nulls_key, 0, LCM_PMI_STRING_LIMIT + 1);
    memset(size_key, 0, LCM_PMI_STRING_LIMIT + 1);

    // Buffers to store published contents
    char encoded_value[LCM_PMI_STRING_LIMIT + 1];
    char nulls_value[LCM_PMI_STRING_LIMIT + 1];
    char size_value[LCM_PMI_STRING_LIMIT + 1];
    memset(encoded_value, 0, LCM_PMI_STRING_LIMIT + 1);
    memset(nulls_value, 0, LCM_PMI_STRING_LIMIT + 1);
    memset(size_value, 0, LCM_PMI_STRING_LIMIT + 1);

    // Set key
    sprintf(encoded_key, "LCI_ENC_%d_%d", endpoint_id, LCI_RANK);
    sprintf(nulls_key, "LCI_NUL_%d_%d", endpoint_id, LCI_RANK);
    sprintf(size_key, "LCI_SIZE_%d_%d", endpoint_id, LCI_RANK);

    // Encode the address
    encode_ucp_address1(my_addrs, addrs_length, encoded_value, nulls_value);
    memcpy(size_value, &addrs_length, sizeof(size_t));

    // Publish values
    lcm_pm_publish(encoded_key, encoded_value);
    lcm_pm_publish(nulls_key, nulls_value);
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
            // Reset keys
            memset(encoded_key, 0, LCM_PMI_STRING_LIMIT + 1);
            memset(nulls_key, 0, LCM_PMI_STRING_LIMIT + 1);
            memset(size_key, 0, LCM_PMI_STRING_LIMIT + 1);

            // Reset values
            memset(encoded_value, 0, LCM_PMI_STRING_LIMIT + 1);
            memset(nulls_value, 0, LCM_PMI_STRING_LIMIT + 1);
            memset(size_value, 0, LCM_PMI_STRING_LIMIT + 1);

            // Set correct keys
            sprintf(encoded_key, "LCI_ENC_%d_%d", endpoint_id, i);
            sprintf(nulls_key, "LCI_NUL_%d_%d", endpoint_id, i);
            sprintf(size_key, "LCI_SIZE_%d_%d", endpoint_id, i);

            // Get values
            lcm_pm_getname(i, encoded_key, encoded_value);
            lcm_pm_getname(i, nulls_key, nulls_value);
            lcm_pm_getname(i, size_key, size_value);

            // Initialize buffer, Decode address
            char decoded_value[LCM_PMI_STRING_LIMIT + 1];
            memset(decoded_value, 0, LCM_PMI_STRING_LIMIT + 1);
            decode_ucp_address1(encoded_value, *((int*)size_value), nulls_value, decoded_value);
            
            // Set peer address
            ep_params.address = (ucp_address_t*) decoded_value;
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