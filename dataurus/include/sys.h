#ifndef __SYS_H__
#define __SYS_H__

#include <unistd.h>
#include <sys/types.h>
#include <asm/unistd.h>
#include <errno.h>

#ifndef NUMAIF_H
#define NUMAIF_H 1

#ifdef __cplusplus
extern "C" {
#endif

/* Kernel interface for NUMA API */
	
/* Policies */
#define MPOL_DEFAULT     0
#define MPOL_PREFERRED    1
#define MPOL_BIND        2
#define MPOL_INTERLEAVE  3

#define MPOL_MAX MPOL_INTERLEAVE

/* Flags for get_mem_policy */
#define MPOL_F_NODE    (1<<0)   /* return next il node or node of address */
				/* Warning: MPOL_F_NODE is unsupported and
				   subject to change. Don't use. */
#define MPOL_F_ADDR     (1<<1)  /* look up vma using address */
#define MPOL_F_MEMS_ALLOWED (1<<2) /* query nodes allowed in cpuset */

/* Flags for mbind */
#define MPOL_MF_STRICT  (1<<0)  /* Verify existing pages in the mapping */
#define MPOL_MF_MOVE	(1<<1)  /* Move pages owned by this process to conform to mapping */
#define MPOL_MF_MOVE_ALL (1<<2) /* Move every page to conform to mapping */
#ifdef __cplusplus
}
#endif

#endif

#define WEAK __attribute__((weak))

long WEAK get_mempolicy(int *policy, const unsigned long *nmask,
				unsigned long maxnode, void *addr, int flags);

#endif