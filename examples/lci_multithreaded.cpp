#include "../src/api/lci.h"
#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <vector>
#include <chrono>
#include <algorithm>
#include <thread>
#include <atomic>
#include <pthread.h>
#include <mutex>
#include <condition_variable>
#include <getopt.h>

using namespace std;

// Rate limit
// Rate is defined as messages / second
double rate = 1000000;
// interval is in microsecond (1e-6)
uint64_t interval = 1000000.0 / rate;

// Define a struct with size 64 byte (prevent false sharing)
template<typename T>
struct alignas(64) alignedAtomic {
    atomic<T> data;
    char* padding[64 - sizeof(atomic<T>)];
    alignedAtomic() {
        atomic<T> data(0);
        memset(padding, 0, 64 - sizeof(atomic<T>) * sizeof(char));
    }
    alignedAtomic(T input) {
        atomic<T> data(input);
        memset(padding, 0, 64 - sizeof(atomic<T>) * sizeof(char));
    }
};

// Number of msg to be sent to each process
vector<alignedAtomic<int>>* sendCounts;
// Records number of msg recved from each process
vector<alignedAtomic<int>>* msgRecved;
// Records number of msg recved from each process
vector<alignedAtomic<int>>* msgSent;
// Message received in this process
alignas(64) atomic<int> nRecv(0);
// Expected number of msg to receive
alignas(64) atomic<int> expectedRecv(0);
// Number of completed threads
alignas(64) atomic<int> nComp(0);

LCI_device_t device;
LCI_endpoint_t ep;
LCI_comp_t cq;

// Version 2 synchronizer
alignas(64) atomic<bool> ready(false);


// Manages send completion status for all other threads
void progressFcn(int n, LCI_device_t device1) {
    while (nComp.load() < n) {
        LCI_progress(device1);
    }
    //printf("finished\n");
}



void threadFcn(unsigned seed) {

    //printf("%d\n", (-1 % LCI_NUM_PROCESSES));
    // Block the thread until all threads are created
    while (ready.load() == false) {

    }

    //printf("\nWorker thread starts: rank %d", LCI_RANK);

    // Timer to limit rate
    uint64_t lastSend = chrono::duration_cast<chrono::microseconds>(chrono::high_resolution_clock::now().time_since_epoch()).count();
    LCI_short_t unused;
    *(uint64_t*)&unused = 1;

    // Temporarily store send and recv number locally
    int tempnRecv = 0;
    int tempMsgSent[LCI_NUM_PROCESSES] = {0};
    int tempMsgRecved[LCI_NUM_PROCESSES] = {0};


    while (true) {

        // Generate a random destination

        // Forced exit for debugging
        uint64_t t1 = chrono::duration_cast<chrono::microseconds>(chrono::high_resolution_clock::now().time_since_epoch()).count();
        if ((t1 - lastSend) / 1000 > 5000) {
            printf("\n stuck in sending\n rank: %d\n recved: %d\n", LCI_RANK, nRecv.load());
            break;
        }

        // Send messages if within rate limit
        uint64_t now = chrono::duration_cast<chrono::microseconds>(chrono::high_resolution_clock::now().time_since_epoch()).count();
        if (now - lastSend > interval) {
            
            // Attempt to find a valid send destination
            int dest = (rand_r(&seed) % LCI_NUM_PROCESSES) - 1;

            // Check for > 0 first
            int newCount = -1;
            int nAttempts = 0;
            int tmp_a = 0;
            while (newCount < 0 && nAttempts < LCI_NUM_PROCESSES + 1) {
                dest = (dest + 1) % LCI_NUM_PROCESSES;
                newCount = --((*sendCounts)[dest].data);
                nAttempts++;
                tmp_a++;
                //printf("\n%d", tmp_a);
                fflush(stdout);
            }

            // No valid destination
            if (nAttempts == LCI_NUM_PROCESSES + 1) {
                break;
            }
            while (LCI_puts(ep, unused, dest, 0, LCI_DEFAULT_COMP_REMOTE) == LCI_ERR_RETRY) {}
            lastSend = chrono::duration_cast<chrono::microseconds>(chrono::high_resolution_clock::now().time_since_epoch()).count();

            // Update send counts
            tempMsgSent[dest]++;

        }

        // Receive message
        if (nRecv < expectedRecv) {
            LCI_request_t request;
            if (LCI_queue_pop(cq, &request) == LCI_OK) {
                tempnRecv++;
                tempMsgRecved[request.rank]++;
            }
        }
    }
    uint64_t sendFinish = chrono::duration_cast<chrono::microseconds>(chrono::high_resolution_clock::now().time_since_epoch()).count();
    nRecv += tempnRecv;

    // Wait for recv to complete
    while (nRecv < expectedRecv) {
        LCI_request_t request;
        if (LCI_queue_pop(cq, &request) == LCI_OK) {
            nRecv++;
            tempMsgRecved[request.rank]++;
        }

        // Forced exit for debugging
        uint64_t t = chrono::duration_cast<chrono::microseconds>(chrono::high_resolution_clock::now().time_since_epoch()).count();
        if ((t - sendFinish) / 1000 > 5000) {
            printf("\n stuck in recv waiting\n rank: %d\n recved: %d\n", LCI_RANK, nRecv.load());
            nRecv.store(expectedRecv);
        }
    }

    // Update global send and recv counts
    for (int i = 0; i < LCI_NUM_PROCESSES; i++) {
        (*msgSent)[i].data += tempMsgSent[i];
        (*msgRecved)[i].data += tempMsgRecved[i];
    }
    nComp++;
}

int main(int argc, char** args) {

    // int f = 0;
    // int z = 0;              
    // while (f == 0) {
    //     z++;
    // }

    // Initialize device and endpoint
    LCI_initialize();

    LCI_device_init(&device);

    // Initialize completion queue
    LCI_queue_create(device, &cq);

    // Set Completion and Send matching mechanisms
    LCI_plist_t plist;

    LCI_plist_create(&plist);

    LCI_plist_set_comp_type(plist, LCI_PORT_COMMAND, LCI_COMPLETION_QUEUE);

    LCI_plist_set_comp_type(plist, LCI_PORT_MESSAGE, LCI_COMPLETION_QUEUE);

    LCI_plist_set_match_type(plist, LCI_MATCH_RANKTAG);

    LCI_plist_set_default_comp(plist, cq);

    LCI_endpoint_init(&ep, device, plist);

    LCI_plist_free(&plist);

    // Initialize global variables
    vector<alignedAtomic<int>> tmp1(LCI_NUM_PROCESSES);
    vector<alignedAtomic<int>> tmp2(LCI_NUM_PROCESSES);
    vector<alignedAtomic<int>> tmp3(LCI_NUM_PROCESSES);
    msgSent = &tmp1;
    msgRecved = &tmp2;
    sendCounts = &tmp3;

    for (int i = 0; i < LCI_NUM_PROCESSES; i++) {
        (*msgRecved)[i].data.store(0);
        (*msgSent)[i].data.store(0);
    }

    // Number of threads
    int nThreads;
    if (thread::hardware_concurrency() != 0) {
        nThreads = thread::hardware_concurrency();
    } else {
        nThreads = 1;
    }

    // Number of messages
    int numMessages = 2000;
    int lower = 100;
    int upper = 3000;

    // Take arguments from command line
    // t: number of threads each process
    // l, u: lower, upper bound of message count
    // m: message count
    // r: message rate
    int next;
    while ((next = getopt(argc, args, "t:l:u:m:r:")) != -1) {
        switch (next)
        {
        case 't': nThreads = atoi(optarg); break;
        case 'l': lower = atoi(optarg); break;
        case 'u': upper = atoi(optarg); break;
        case 'm': numMessages = atoi(optarg); break;
        case 'r': interval = 1000000.0 / atoi(optarg); break;
        
        default: printf("unknown arguments\n"); break;
        }
    }

    // Determine if given lower and upper are possible
    assert(LCI_NUM_PROCESSES * lower <= numMessages);
    assert(LCI_NUM_PROCESSES * upper >= numMessages);

    // Number of message to send to each process
    srand(time(NULL) + LCI_RANK);
    for (int i = 0; i < LCI_NUM_PROCESSES; i++) {

        // Calculate a random number within limit
        if (i == LCI_NUM_PROCESSES - 1) {
            (*sendCounts)[i].data.store(numMessages);
        } else {

            // Make sure the current and future random number can meet upper and lower requirements
            int remaining = LCI_NUM_PROCESSES - i - 1;
            int tempLower = numMessages - upper * remaining;
            int tempUpper = numMessages - lower * remaining;
            int newLower = max(tempLower, lower);
            int newUpper = min(tempUpper, upper);
            int count = (rand() % (newUpper - newLower + 1)) + newLower;
            (*sendCounts)[i].data.store(count);
            numMessages = numMessages - count;
        }
    }

    // Send expected message counts to all processes
    // Message count is sent in the tag of the message (corrupted memory when sending with data)
    LCI_short_t src;
    for (int i = LCI_RANK; i < LCI_RANK + LCI_NUM_PROCESSES; i++) {
        *(uint32_t*)&src = (uint32_t) 1;
        while (LCI_puts(ep, src, i % LCI_NUM_PROCESSES, (*sendCounts)[i % LCI_NUM_PROCESSES].data.load(), LCI_DEFAULT_COMP_REMOTE) == LCI_ERR_RETRY) {
            LCI_progress(device);
        }
    }

    // Recv expected message counts
    int tempRecved = 0;
    while (tempRecved < LCI_NUM_PROCESSES) {
        LCI_request_t request;
        while (LCI_queue_pop(cq, &request) == LCI_ERR_RETRY) {
            LCI_progress(device);
            
        }
        tempRecved++;

        // Write expected number of recved message
        expectedRecv += request.tag;
    }

    // Progress thread
    thread progresser(progressFcn, nThreads, device);

    // Create threads to send and recv
    vector<thread> ts;
    for (int i = 0; i < nThreads; i++) {
        ts.push_back(thread(threadFcn, time(NULL) + i * LCI_NUM_PROCESSES + LCI_RANK));
    }

    // Signal all threads to start, record time
    uint64_t sendStart = chrono::duration_cast<chrono::microseconds>(chrono::high_resolution_clock::now().time_since_epoch()).count();
    ready.store(true);
    for (int i = 0; i < nThreads; i++) {
        ts[i].join();
    }

    progresser.join();
    uint64_t recvFinish = chrono::duration_cast<chrono::microseconds>(chrono::high_resolution_clock::now().time_since_epoch()).count();

    // Message count for correctness
    for (int r = 0; r < LCI_NUM_PROCESSES; r++) {
        printf("process %d sent to process %d: %d \n", LCI_RANK, r, (*msgSent)[r].data.load());
        printf("process %d received from process %d: %d \n", LCI_RANK, r, (*msgRecved)[r].data.load());
    }

    // Total time to send and recv messages
    printf("Total time: %f ms \n", (recvFinish - sendStart) / 1000.0);

    // Finalize LCI environment
    LCI_finalize();
}
