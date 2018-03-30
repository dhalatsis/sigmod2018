/*
 * year:2018 month:03 day:28
 *
 * The author disclaims copyright to this source code.
 * In place of a legal notice, here is a blessing
 * as indicated at the SQLite's source code:
 *
 *    May you do good and not evil.
 *    May you find forgiveness for yourself and forgive others.
 *    May you share freely, never taking more than you give.
 *
 * **********************************************************************
 */

// #include "threadpool11/threadpool11.hpp"

#include <sys/time.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdint.h>
#include <time.h>
#include <vector>

// Pthread libs
#include <pthread.h>
#include <sched.h>

#define NUM_ROWS 100000000

typedef struct arg_t {
  uint64_t* column;
  int rows;
  uint64_t value;
  uint64_t* result;
} arg_t;

void vector_filter_less(uint64_t *column,
                        size_t num_rows,
                        uint64_t value,
                        std::vector<uint64_t>& res) {
  for (int i = 0; i < num_rows; ++i) {
    if (column[i] < value) {
      res.push_back(column[i]);
    }
  }
}

uint64_t* array_2pass_filter_less(uint64_t const * const column,
                                  size_t num_rows,
                                  uint64_t value) {
//  printf("%ld\n", num_rows);
  int count = 0;
  for (int i = 0; i < num_rows; ++i) {
    count += (column[i] < value);
  }

  uint64_t *res = (uint64_t*) malloc(count * sizeof(uint64_t));
  //for (int k=0 ; k < 20 ; k++){
  int j = 0;
  for (int i = 0; i < num_rows; ++i) {
    if (column[i] < value) {
      res[j++] = column[i];
    }
  }
  //}
  //printf("DONE!\n");
  return res;
}

void* thread_filter(void* param) {
  arg_t* args = (arg_t*) param;
  args->result = array_2pass_filter_less(args->column, args->rows, args->value);
}

int main() {
  struct timeval startt, endt;
  uint64_t *column = (uint64_t*) malloc(NUM_ROWS * sizeof(uint64_t));
  int domain = (NUM_ROWS / 10.0);
  float selectivity = 0.01;

  for (int i = 0; i < NUM_ROWS; ++i) {
    column[i] = (uint64_t) (rand() % domain);
  }

  /*
  // Vector Filter default initial capacity
  {
    printf("Vector Filter default capacity :: ");
    uint64_t v = rand() % 1 + domain * selectivity;
    t = clock();
    for (int i = 0; i < 10; ++i) {
      std::vector<uint64_t> res;
      vector_filter_less(column, NUM_ROWS, v, res);
    }
    t = clock() - t;
    printf ("It took me %ld clicks (%f seconds).\n",
            t, ((float) t) / CLOCKS_PER_SEC);
  }

  // Vector Filter initial capacity 1000
  {
    printf("Vector Filter initial capacity 1000 :: ");
    uint64_t v = rand() % 1 + domain * selectivity;
    t = clock();
    for (int i = 0; i < 10; ++i) {
      std::vector<uint64_t> res(1000);
      vector_filter_less(column, NUM_ROWS, v, res);
    }
    t = clock() - t;
    printf ("It took me %ld clicks (%f seconds).\n",
            t, ((float) t) / CLOCKS_PER_SEC);
  }

  // Vector Filter exact initial capacity
  {
    printf("Vector Filter initial capacity exact :: ");
    uint64_t v = rand() % 1 + domain * selectivity;
    t = clock();
    for (int i = 0; i < 10; ++i) {
      std::vector<uint64_t> res(selectivity * NUM_ROWS);
      vector_filter_less(column, NUM_ROWS, v, res);
    }
    t = clock() - t;
    printf ("It took me %ld clicks (%f seconds).\n",
            t, ((float) t) / CLOCKS_PER_SEC);
  }
  */

  // Array Filter 2 passes
  {
    printf("Array Filter 2 passes :: \n");
    uint64_t v = domain * selectivity;
    gettimeofday(&startt, NULL);

    for (int i = 0; i < 1; ++i) {
      uint64_t *res = array_2pass_filter_less(column, NUM_ROWS, v);
      free(res);
    }
    gettimeofday(&endt, NULL);
    printf ("\tIt took me %lf seconds).\n",
            (endt.tv_sec - startt.tv_sec) + (endt.tv_usec - startt.tv_usec) / 1000000.0);
  }

  /*
  // Threadpool array filter 2 passes
  {
    printf("Threadpool array filter 2 passes :: ");
    threadpool11::Pool pool;
    uint64_t v = domain * selectivity;
    t = clock();
    for (int iterations = 0; iterations < 1; ++iterations) {
      std::vector<std::future<uint64_t*>> futures;
      int chunck_number = 2;
      int chunck_size = NUM_ROWS / (float) chunck_number;
      for (int i = 0; i < chunck_number - 1; ++i) {
        futures.emplace_back(pool.postWork<uint64_t*>([=]() {
              return array_2pass_filter_less(column + i * chunck_size,
                                             chunck_size, v);
            }));
      }
      futures.emplace_back(pool.postWork<uint64_t*>([=]() {
            int start = (chunck_number - 1) * chunck_size;
            return array_2pass_filter_less(column + start, NUM_ROWS - start, v);
          }));

      for (auto& it : futures) {
        free(it.get());
      }
    }
    t = clock() - t;
    printf ("It took me %ld clicks (%f seconds).\n",
            t, ((float) t) / CLOCKS_PER_SEC);
  }

  // C++11 threads array filter 2 passes
  {
    printf("C++11 threads array filter 2 passes :: ");
    uint64_t v = domain * selectivity;
    int chunck_number = 2;
    int chunck_size = NUM_ROWS / (float) chunck_number;
    std::vector<std::thread> threads(chunck_number);
    uint64_t* res[chunck_number];

    t = clock();
    for (int i = 0; i < chunck_number - 1; ++i) {
      threads[i] = std::thread([=, &res]() {
          // printf("Thread #%d on CPU %d\n", i, sched_getcpu());
          res[i] = array_2pass_filter_less(column + i * chunck_size,
                                             chunck_size, v);
        });
    }
    threads[chunck_number - 1] = std::thread([=, &res]() {
        // printf("Thread #%d on CPU %d\n", chunck_number - 1, sched_getcpu());
        int start = (chunck_number - 1) * chunck_size;
        res[chunck_number - 1] = array_2pass_filter_less(column + start,
                                                         NUM_ROWS - start,
                                                         v);
      });

    for (auto& t : threads) {
      t.join();
    }

    t = clock() - t;
    printf ("It took me %ld clicks (%f seconds).\n",
            t, ((float) t) / CLOCKS_PER_SEC);
  }
  */

  // Pthreads array filter 2 passes
  {
    printf("Pthreads array filter 2 passes :: \n");

    // Chunk variables
    int chunck_number = 4;
    uint64_t v = domain * selectivity;
    int chunck_size = NUM_ROWS / (float) chunck_number;
    int cpus[] = {0, 1, 2, 3};//int cpus[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19};

    // Pthreads variables
    pthread_t tid[chunck_number];
    pthread_attr_t attr;
    cpu_set_t set;
    arg_t args[chunck_number];

    // Initialize arguments
    pthread_attr_init(&attr);
    for (int i = 0; i < chunck_number - 1; ++i) {
      args[i].column = column + i * chunck_size;
      args[i].rows = chunck_size;
      args[i].value = domain;
    }
    int last = chunck_number - 1;
    int start = last * chunck_size;
    args[last].column = column + start;
    args[last].rows = NUM_ROWS - start;
    args[last].value = domain;

    // Execute threads and create numa regions
    gettimeofday(&startt, NULL);
    for (int i = 0; i < chunck_number; ++i) {
      int cpu_idx = cpus[i];

      CPU_ZERO(&set);
      CPU_SET(cpu_idx, &set);
      pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &set);

      int rv = pthread_create(&tid[i], &attr, thread_filter, (void*) &args[i]);
      if (rv) {
        printf("[ERROR] return code from pthread_create is %d\n", rv);
        exit(-1);
      }
    }

    // Exchange the columns with the local numa regions
    for (int i = 0; i < chunck_number; ++i) {
      pthread_join(tid[i], NULL);
      // free(args[i].result);
      args[i].column = args[i].result;
    }
    gettimeofday(&endt, NULL);
    printf ("\tIt took me %lf seconds.\n",
           (endt.tv_sec - startt.tv_sec) + (endt.tv_usec - startt.tv_usec) / 1000000.0);

    // Numa filter
    printf("Numa optimized filter ::\n");
    gettimeofday(&startt, NULL);
    for (int i = 0; i < chunck_number; ++i) {
      int cpu_idx = cpus[i];

      CPU_ZERO(&set);
      CPU_SET(cpu_idx, &set);
      pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &set);

      args[i].value = domain * selectivity;

      int rv = pthread_create(&tid[i], &attr, thread_filter, (void*) &args[i]);
      if (rv) {
        printf("[ERROR] return code from pthread_create is %d\n", rv);
        exit(-1);
      }
    }

    // Free columns and results
    for (int i = 0; i < chunck_number; ++i) {
      pthread_join(tid[i], NULL);
      free(args[i].column);
      free(args[i].result);
    }
    gettimeofday(&endt, NULL);
    printf ("\tIt took me %lf seconds.\n",
            (endt.tv_sec - startt.tv_sec) + (endt.tv_usec - startt.tv_usec) / 1000000.0);
  }
}
