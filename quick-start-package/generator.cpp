/* @version $Id: generator.c 5255 2014-07-11 15:31:34Z bcagri $ */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <sched.h>              /* CPU_ZERO, CPU_SET */
#include <pthread.h>            /* pthread_attr_setaffinity_np */
#include <stdio.h>              /* perror */
#include <stdlib.h>             /* posix_memalign */
#include <math.h>               /* fmod, pow */
#include <time.h>               /* time() */
#include <unistd.h>             /* getpagesize() */
#include <string.h>             /* memcpy() */

#include "cpu_mapping.h"        /* get_cpu_id() */
#include "generator.h"          /* create_relation_*() */
#include "affinity.h"           /* pthread_attr_setaffinity_np */
#include "lock.h"
#include "prj_params.h"         /* RELATION_PADDING for Parallel Radix */

/* return a random number in range [0,N] */
#define RAND_RANGE(N) ((double)rand() / ((double)RAND_MAX + 1) * (N))
#define RAND_RANGE48(N,STATE) ((double)nrand48(STATE)/((double)RAND_MAX+1)*(N))
#define MALLOC(SZ) alloc_aligned(SZ+RELATION_PADDING) /*malloc(SZ+RELATION_PADDING)*/
#define FREE(X,SZ) free(X)

#ifndef BARRIER_ARRIVE
/** barrier wait macro */
#define BARRIER_ARRIVE(B,RV)                            \
    RV = pthread_barrier_wait(B);                       \
    if(RV !=0 && RV != PTHREAD_BARRIER_SERIAL_THREAD){  \
        printf("Couldn't wait on barrier\n");           \
        exit(EXIT_FAILURE);                             \
    }
#endif

/** An experimental feature to allocate input relations numa-local */
int numalocalize;
int nthreads;

static int seeded = 0;
static unsigned int seedValue;

void *
alloc_aligned(size_t size)
{
    void * ret;
    int rv;
    rv = posix_memalign((void**)&ret, CACHE_LINE_SIZE, size);

    if (rv) {
        perror("[ERROR] alloc_aligned() failed: out of memory");
        return 0;
    }

    /** Not an elegant way of passing whether we will numa-localize, but this
        feature is experimental anyway. */
    if(numalocalize) {
        tuple_t * mem     = (tuple_t *) ret;
        uint64_t  ntuples = size / sizeof(tuple_t);
        numa_localize(mem, ntuples, nthreads);
    }

    return ret;
}

// ADDED ***always remember the padding for L1 misses */
relation_t *
gen_rel(int num_tuples)
{
    relation_t * r1 = (relation_t *) malloc(sizeof(relation_t));
    r1->num_tuples = num_tuples;
    r1->tuples = (tuple_t *)MALLOC(sizeof(tuple_t) * num_tuples);
    return r1;
}

struct create_arg_t {
    relation_t          rel;
    int64_t             firstkey;
    int64_t             maxid;
    uint64_t            ridstart;
    relation_t *        fullrel;
    volatile void *     locks;
    pthread_barrier_t * barrier;
};

typedef struct create_arg_t create_arg_t;

/**
 * Just initialize mem. to 0 for making sure it will be allocated numa-local
 */
void *
numa_localize_thread(void * args)
{
    create_arg_t * arg = (create_arg_t *) args;
    relation_t *   rel = & arg->rel;
    uint64_t i;

    for (i = 0; i < rel->num_tuples; i++) {
        rel->tuples[i].key = 0;
    }

    return 0;
}


int
numa_localize(tuple_t * relation, int64_t num_tuples, uint32_t nthreads)
{
    uint32_t i, rv;
    uint64_t offset = 0;

    /* we need aligned allocation of items */
    create_arg_t args[nthreads];
    pthread_t tid[nthreads];
    cpu_set_t set;
    pthread_attr_t attr;

    unsigned int pagesize;
    unsigned int npages;
    unsigned int npages_perthr;
    uint64_t ntuples_perthr;
    uint64_t ntuples_lastthr;

    pagesize        = getpagesize();
    npages          = (num_tuples * sizeof(tuple_t)) / pagesize + 1;
    npages_perthr   = npages / nthreads;
    ntuples_perthr  = npages_perthr * (pagesize/sizeof(tuple_t));
    ntuples_lastthr = num_tuples - ntuples_perthr * (nthreads-1);

    pthread_attr_init(&attr);

    for( i = 0; i < nthreads; i++ ) {
        int cpu_idx = get_cpu_id(i);

        CPU_ZERO(&set);
        CPU_SET(cpu_idx, &set);
        pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &set);

        args[i].firstkey       = offset + 1;
        args[i].rel.tuples     = relation + offset;
        args[i].rel.num_tuples = (i == nthreads-1) ? ntuples_lastthr
                                 : ntuples_perthr;
        offset += ntuples_perthr;

        rv = pthread_create(&tid[i], &attr, numa_localize_thread,
                            (void*)&args[i]);
        if (rv){
            fprintf(stderr, "[ERROR] pthread_create() return code is %d\n", rv);
            exit(-1);
        }
    }

    for(i = 0; i < nthreads; i++){
        pthread_join(tid[i], NULL);
    }

    return 0;
}
