#include <stdlib.h> /* exit, perror */
#include <unistd.h> /* sysconf */
#include <errno.h>
#include "cpu_mapping.h"
/*--------------------------$WARNING$ FOR THE DEFINE CHECK cpu_mapping.h-------------------------*/



#define MAX_NODES 0
/*-----------------------------------------------WARNING HARDCODED-------------------------------*/
#ifdef MY_PC
static int numas = 1;
static int threads_per_numa = 4;
static int numa[][4] = {0,1,2,3};
static int cpu_mapping[] = {0, 1, 2, 3};
#endif

#ifdef SIGMOD_1CPU 1 /*<---------------------------ALWAYS DEFIEND IT BEFORE UPLOAD-------------------*/
static int numas = 1;
static int threads_per_numa = 20;
static int numa[][20] = {
        {0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38}
};
static int cpu_mapping[] = {0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38};
#endif

#ifdef SIGMOD_2CPU
static int numas = 2;
static int threads_per_numa = 20;
static int numa[][20] = {
    {0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38},
    {1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23, 25, 27, 29, 31, 33, 35, 37, 39}
};
static int cpu_mapping[] = {0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38,
                        1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23, 25, 27, 29, 31, 33, 35, 37, 39
};
#endif
/*-----------------------------------------------END OF HARDCODED----------------------------------*/

/**
 * Returns SMT aware logical to physical CPU mapping for a given thread id.
 */
int
get_cpu_id(int thread_id)
{
    return cpu_mapping[thread_id % threads_per_numa];
}

int
get_numa_id(int mytid)
{
    int ret = 0;
    for(int i = 0; i < numas; i++)
        for(int j = 0; j < threads_per_numa; j++)
            if(numa[i][j] == mytid){
                ret = i;
                break;
            }

    return ret;
}

int
get_num_numa_regions(void)
{
    return numas;
}

int
get_numa_node_of_address(void * ptr)
{
    return MAX_NODES;
}
