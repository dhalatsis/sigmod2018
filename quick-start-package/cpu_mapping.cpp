/* @version $Id: cpu_mapping.c 4548 2013-12-07 16:05:16Z bcagri $ */

#include <stdio.h>  /* FILE, fopen */
#include <stdlib.h> /* exit, perror */
#include <unistd.h> /* sysconf */
#include <errno.h>

#include "cpu_mapping.h"

/** \internal
 * @{
 */
//#define my_Topology
#define MAX_NODES 512

static int inited = 0;
static int max_cpus;
static int node_mapping[MAX_NODES];

/**
 * Initializes the cpu mapping from the file defined by CUSTOM_CPU_MAPPING.
 * The mapping used for our machine Intel L5520 is = "8 0 1 2 3 8 9 10 11".
 */
static int
init_mappings_from_file()
{
    FILE * cfg;
	int i;

    cfg = fopen(CUSTOM_CPU_MAPPING, "r");
    if (cfg!=NULL) {
        if(fscanf(cfg, "%d", &max_cpus) <= 0) {
            perror("Could not parse input!\n");
        }

        for(i = 0; i < max_cpus; i++){
            if(fscanf(cfg, "%d", &node_mapping[i]) <= 0) {
                perror("Could not parse input!\n");
            }
        }

        fclose(cfg);
        return 1;
    }


    /* perror("Custom cpu mapping file not found!\n"); */
    return 0;
}

/**
 * Try custom cpu mapping file first, if does not exist then round-robin
 * initialization among available CPUs reported by the system.
 */
static void
init_mappings()
{
    if( init_mappings_from_file() == 0 ) {
        int i;

        max_cpus  = sysconf(_SC_NPROCESSORS_ONLN);
        for(i = 0; i < max_cpus; i++){
            node_mapping[i] = i;
        }
    }
}

/** @} */

/**
 * Returns SMT aware logical to physical CPU mapping for a given thread id.
 */
int
get_cpu_id(int thread_id)
{
    if(!inited){
        init_mappings();
        inited = 1;
    }

    return node_mapping[thread_id % max_cpus];
}

/* TODO: These are just place-holder implementations. */
/**
 * Topology of Intel E5-4640
 node 0 cpus: 0 4 8 12 16 20 24 28 32 36 40 44 48 52 56 60
 node 1 cpus: 1 5 9 13 17 21 25 29 33 37 41 45 49 53 57 61
 node 2 cpus: 2 6 10 14 18 22 26 30 34 38 42 46 50 54 58 62
 node 3 cpus: 3 7 11 15 19 23 27 31 35 39 43 47 51 55 59 63
*/
#define INTEL_E5 1


#ifdef my_Topology

/* TODO CHANGE
static int numa[][16] = {
    {0, 4, 8, 12, 16, 20, 24, 28, 32, 36, 40, 44, 48, 52, 56, 60},
    {1, 5, 9, 13, 17, 21, 25, 29, 33, 37, 41, 45, 49, 53, 57, 61},
    {2, 6, 10, 14, 18, 22, 26, 30, 34, 38, 42, 46, 50, 54, 58, 62},
    {3, 7, 11, 15, 19, 23, 27, 31, 35, 39, 43, 47, 51, 55, 59, 63} };
*/

static int numa[][4] = {{0,1,2,3}};

int
get_numa_id(int mytid)
{
#if INTEL_E5
    int ret = 0;

    for(int i = 0; i < 1; i++)
        for(int j = 0; j < 4; j++)
            if(numa[i][j] == mytid){
                ret = i;
                break;
            }

    return ret;
#else
    return 0;
#endif
}

int
get_num_numa_regions(void)
{
    /* TODO: FIXME automate it from the system config. */
#if INTEL_E5
    return 1;//return 4; TODO CHANGE
#else
    return 1;
#endif
}

#else  /* SIGMOD PC */

/* ----------------SIGMODS PC --------*/
/* 2 NODES,offset 4 */
static int numa[][20] = {
    {0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38},
    {1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23, 25, 27, 29, 31, 33, 35, 37, 39}};

int
get_numa_id(int mytid)
{
#if INTEL_E5
    int ret = 0;

    for(int i = 0; i < 2; i++)
        for(int j = 0; j < 20; j++)
            if(numa[i][j] == mytid){
                ret = i;
                break;
            }

    return ret;
#else
    return 0;
#endif
}

int
get_num_numa_regions(void)
{
    /* TODO: FIXME automate it from the system config. */
#if INTEL_E5
    return 2; /* sigmods pc */
    //return 1; /* my pc */
#else
    return 1;
#endif
}

#endif  /*---------------- END OF SIGMOD TOPOLOGY ---------------- */

int
get_numa_node_of_address(void * ptr)
{
    int numa_node = 0;
    return numa_node;
}
