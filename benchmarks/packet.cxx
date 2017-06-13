#include "comm_exp.h"
#include "mv.h"
#include "pool.h"

#define USE_PAPI
#include "mv/profiler.h"

#include "packet/packet_manager_misc.h"

__thread int mv_core_id = -1;

#include <atomic>
#include <chrono>
#include <thread>
#include <algorithm>
#include <vector>

int NTHREADS = 4;
int PER_THREAD = 16;

int* cache_buf;
int cache_size;

static void cache_invalidate(void)
{
  int i;
  cache_buf[0] = 1;
  for (i = 1; i < cache_size; ++i) {
    cache_buf[i] = cache_buf[i - 1];
  }
}

void benchmarks()
{
  mv_pool* pkg;
  int* data = (int*) malloc(4 * MAX_PACKET);
  mv_pool_create(&pkg, data, 4, MAX_PACKET);

  std::atomic<int> f;
  std::vector<double> times(TOTAL_LARGE - SKIP_LARGE, 0);

  srand(1234);
  std::vector<int> rands;
  std::thread th[NTHREADS];
  PER_THREAD = MAX_PACKET / (MAX_PACKET / NTHREADS);
  profiler_init();
  profiler prof({PAPI_L2_TCM});
  double l1 = 0;

  for (int k = 0; k < TOTAL_LARGE; k++) {
    rands.clear();
    for (int i = 0; i < PER_THREAD * (NTHREADS); i++) {
      rands.push_back(rand() % (MAX_PACKET / NTHREADS) + 1);
    }
    cache_invalidate();
    f = 0;
    for (int i = 0; i < NTHREADS; i++) {
      th[i] = std::move(std::thread([&, i] {
        std::vector<void*> pp(MAX_PACKET);
        int ff = f.fetch_add(1);
        set_me_to_(ff);
        while (f < NTHREADS) {
        }
        double t1 = 0;
        if (i == 0 && k >= SKIP_LARGE) { 
          t1 = wutime();
          prof.start();
        }

        int sumnrun = 0;
        for (int j = 0; j < PER_THREAD; j++) {
          int nruns = rands[i * PER_THREAD + j];
          for (int jj = 0; jj < nruns; jj++) {
            pp[jj] = mv_pool_get(pkg);
          }
          if (k >= SKIP_LARGE && i == 0) times[k - SKIP_LARGE] += wutime();
          std::this_thread::sleep_for(
              std::chrono::microseconds(rands[i * PER_THREAD]));
          if (k >= SKIP_LARGE && i == 0) times[k - SKIP_LARGE] -= wutime();

          for (int jj = 0; jj < nruns; jj++) {
            mv_pool_put(pkg, pp[jj]);
          }
          sumnrun += nruns;
        }

        if (i == 0 && k >= SKIP_LARGE) {
          // printf("%d %.5f\n", PER_THREAD, times[k - SKIP_LARGE]);
          times[k - SKIP_LARGE] += ((wutime() - t1));
          times[k - SKIP_LARGE] /= sumnrun;
          auto& s = prof.stop();
          l1 += (double) s[0] / sumnrun;
        }
      }));
    }
    for (int i = 0; i < NTHREADS; i++) th[i].join();
  }

  int size = times.size();
  std::sort(times.begin(), times.end());
  std::vector<double> qu(5, 0.0);
  qu[0] = (size % 2 == 1)
              ? (times[size / 2])
              : ((times[size / 2 - 1] + times[size / 2]) / 2);  // median
  qu[1] = times[size * 3 / 4];                                  // u q
  qu[2] = times[size / 4];                                      // d q
  double iq = qu[1] - qu[2];
  qu[3] = std::min(qu[1] + 1.5 * iq, times[size - 1]);  // max
  qu[4] = std::max(qu[2] - 1.5 * iq, times[0]);         // min

  // for (auto &q : qu) q = q / PER_THREAD;

  printf("Time (get+return): %d %.3f %.3f %.3f %.3f %.3f\n", NTHREADS, qu[0],
         qu[1], qu[2], qu[3], qu[4]);
  printf("Cache: %.5f\n", l1 / TOTAL_LARGE);
}

template <class T>
void benchmarks_old()
{
  T pkg;
  pkg.init_worker(NTHREADS);
  for (int i = 0; i < MAX_PACKET; i++) {
    pkg.ret_packet(new int());
  }
  std::atomic<int> f;
  std::vector<double> times(TOTAL_LARGE - SKIP_LARGE, 0);

  srand(1234);
  std::vector<int> rands;
  std::thread th[NTHREADS];
  PER_THREAD = MAX_PACKET / (MAX_PACKET / NTHREADS);
  for (int k = 0; k < TOTAL_LARGE; k++) {
    rands.clear();
    for (int i = 0; i < PER_THREAD * (NTHREADS); i++) {
      rands.push_back(rand() % (MAX_PACKET / NTHREADS) + 1);
    }
    cache_invalidate();
    f = 0;
    for (int i = 0; i < NTHREADS; i++) {
      th[i] = std::move(std::thread([&] {
        std::vector<void*> pp(MAX_PACKET);
        int ff = f.fetch_add(1);
        set_me_to_(ff);
        while (f < NTHREADS) {
        }
        double t1 = 0;
        if (ff == 0 && k >= SKIP_LARGE) t1 = wutime();
        int sumnrun = 0;
        for (int j = 0; j < PER_THREAD; j++) {
          int nruns = rands[ff * PER_THREAD + j];
          for (int jj = 0; jj < nruns; jj++) {
            pp[jj] = pkg.get_for_send();
          }
          if (k >= SKIP_LARGE && ff == 0) times[k - SKIP_LARGE] += wutime();
          std::this_thread::sleep_for(
              std::chrono::microseconds(rands[ff * PER_THREAD]));
          if (k >= SKIP_LARGE && ff == 0) times[k - SKIP_LARGE] -= wutime();

          for (int jj = 0; jj < nruns; jj++) {
            pkg.ret_packet_to(pp[jj], ff);
          }
          sumnrun += nruns;
        }
        if (ff == 0 && k >= SKIP_LARGE) {
          times[k - SKIP_LARGE] += ((wutime() - t1));
          times[k - SKIP_LARGE] /= sumnrun;
        }
      }));
    }
    for (int i = 0; i < NTHREADS; i++) th[i].join();
  }

  int size = times.size();
  std::sort(times.begin(), times.end());
  std::vector<double> qu(5, 0.0);
  qu[0] = (size % 2 == 1)
              ? (times[size / 2])
              : ((times[size / 2 - 1] + times[size / 2]) / 2);  // median
  qu[1] = times[size * 3 / 4];                                  // u q
  qu[2] = times[size / 4];                                      // d q
  double iq = qu[1] - qu[2];
  qu[3] = std::min(qu[1] + 1.5 * iq, times[size - 1]);  // max
  qu[4] = std::max(qu[2] - 1.5 * iq, times[0]);         // min

  // for (auto &q : qu) q = q / PER_THREAD;

  printf("Time (get+return): %d %.3f %.3f %.3f %.3f %.3f\n", NTHREADS, qu[0],
         qu[1], qu[2], qu[3], qu[4]);
}

int main(int argc, char** args)
{
  if (argc > 1) NTHREADS = atoi(args[1]);
  cache_size = (8 * 1024 * 1024 / sizeof(int));
  cache_buf = (int*)malloc(sizeof(int) * cache_size);
  mv_pool_init();
  benchmarks();

  // benchmarks_cp<packet_manager_NUMA_STEAL>();
  // benchmarks_cp<packet_manager_LFSTACK>();
  // benchmarks_cp<packet_manager_MPMCQ>();
  benchmarks_old<packetManagerLfQueue>();
  benchmarks_old<packetManagerMPMCQ>();
  return 0;
}

void main_task(intptr_t) {}
