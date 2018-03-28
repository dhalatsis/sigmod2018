#include "Joiner.hpp"
#include "tbb_parallel_types.hpp"

#include <vector>
#include "tbb/tbb.h"
#include "tbb/parallel_reduce.h"
#include "tbb/parallel_for.h"
#include "tbb/blocked_range.h"

// generally it's better to be avoided but let it be here...
using namespace tbb;
using namespace std;

unsigned new_tbi;
spin_mutex FilterMutex;

/*----------- Struct to parallelize filter  ----------------*/
/* Create Relation T parallel ctruct */
struct ParallelItermediateEqualFilterT {
    uint64_t * values;
    unsigned * old_rids;
    int filter;
    unsigned new_tbi;
    unsigned * rids;
    uint64_t size;

    /* Initial constructor */
    ParallelItermediateEqualFilterT ( uint64_t * values, unsigned * old_rids, /*unsigned * rids,*/ int filter )
    : values{values}, old_rids{old_rids}, rids{NULL}, filter{filter}, new_tbi(0), size(0)
    {}

    /* Slpitting constructor */
    ParallelItermediateEqualFilterT(ParallelItermediateEqualFilterT & x, split)
    : values{x.values}, old_rids{x.old_rids}, rids{NULL}, filter{x.filter}, new_tbi(0), size(0)
    {}

    /* The function call overloaded operator */
    void operator()(const tbb::blocked_range<size_t>& range) {
        /* Do the write */
        // spin_mutex::scoped_lock lock;
        for (size_t i = range.begin(); i < range.end(); ++i) {
            if (values[old_rids[i]] == filter) {
                // lock.acquire(FilterMutex);
                if (new_tbi == size) {
                    size += 1000;
                    rids = (unsigned*) realloc(rids, size * sizeof(unsigned));
                }
                rids[new_tbi] = old_rids[i];
                new_tbi++;
                // lock.release();
            }
        }
    }

    /* The function to call on thread join */
    void join( ParallelItermediateEqualFilterT& rhs ) {
        if (rhs.new_tbi) {
            // unsigned * result = new unsigned[new_tbi + rhs.new_tbi];
            unsigned * result = (unsigned*) malloc((new_tbi + rhs.new_tbi) * sizeof(unsigned));
            copy(rids, rids + new_tbi, result);
            free (rids); // we copied them, we are done with them
            copy(rhs.rids, rhs.rids + rhs.new_tbi, result + new_tbi);
            size += rhs.size;
            rids = result;
            new_tbi += rhs.new_tbi;
        }
    }
};

/* Create Relation T parallel ctruct */
struct ParallelNonItermediateEqualFilterT {
    uint64_t * values;
    unsigned * old_rids;
    int filter;
    unsigned new_tbi;
    unsigned * rids;
    uint64_t size;

    /* Initial constructor */
    ParallelNonItermediateEqualFilterT ( uint64_t * values, unsigned * old_rids, /*unsigned * rids,*/ int filter )
    : values{values}, old_rids{old_rids}, rids{NULL}, filter{filter}, new_tbi(0), size(0)
    {}

    /* Slpitting constructor */
    ParallelNonItermediateEqualFilterT(ParallelNonItermediateEqualFilterT & x, split)
    : values{x.values}, old_rids{x.old_rids}, rids{NULL}, filter{x.filter}, new_tbi(0), size(0)
    {}

    /* The function call overloaded operator */
    void operator()(const tbb::blocked_range<size_t>& range) {
        /* Do the write */
        // spin_mutex::scoped_lock lock;
        for (size_t i = range.begin(); i < range.end(); ++i) {
            if (values[i] == filter) {
                // lock.acquire(FilterMutex);
                if (new_tbi == size) {
                    size += 1000;
                    rids = (unsigned*) realloc(rids, size * sizeof(unsigned));
                }
                rids[new_tbi] = i;
                new_tbi++;
                // lock.release();
            }
        }
    }

    /* The function to call on thread join */
    void join( ParallelNonItermediateEqualFilterT& rhs ) {
        if (rhs.new_tbi) {
            // unsigned * result = new unsigned[new_tbi + rhs.new_tbi];
            unsigned * result = (unsigned*) malloc((new_tbi + rhs.new_tbi) * sizeof(unsigned));
            copy(rids, rids + new_tbi, result);
            free (rids); // we copied them, we are done with them
            copy(rhs.rids, rhs.rids + rhs.new_tbi, result + new_tbi);
            size += rhs.size;
            rids = result;
            new_tbi += rhs.new_tbi;
        }
    }
};

/* Create Relation T parallel ctruct */
struct ParallelItermediateGreaterFilterT {
    uint64_t * values;
    unsigned * old_rids;
    int filter;
    unsigned new_tbi;
    unsigned * rids;
    uint64_t size;

    /* Initial constructor */
    ParallelItermediateGreaterFilterT ( uint64_t * values, unsigned * old_rids, /*unsigned * rids,*/ int filter )
    : values{values}, old_rids{old_rids}, rids{NULL}, filter{filter}, new_tbi(0), size(0)
    {}

    /* Slpitting constructor */
    ParallelItermediateGreaterFilterT(ParallelItermediateGreaterFilterT & x, split)
    : values{x.values}, old_rids{x.old_rids}, rids{NULL}, filter{x.filter}, new_tbi(0), size(0)
    {}

    /* The function call overloaded operator */
    void operator()(const tbb::blocked_range<size_t>& range) {
        /* Do the write */
        // spin_mutex::scoped_lock lock;
        for (size_t i = range.begin(); i < range.end(); ++i) {
            if (values[old_rids[i]] > filter) {
                // lock.acquire(FilterMutex);
                if (new_tbi == size) {
                    size += 1000;
                    rids = (unsigned*) realloc(rids, size * sizeof(unsigned));
                }
                rids[new_tbi] = old_rids[i];
                new_tbi++;
                // lock.release();
            }
        }
    }

    /* The function to call on thread join */
    void join( ParallelItermediateGreaterFilterT& rhs ) {
        if (rhs.new_tbi) {
            // unsigned * result = new unsigned[new_tbi + rhs.new_tbi];
            unsigned * result = (unsigned*) malloc((new_tbi + rhs.new_tbi) * sizeof(unsigned));
            copy(rids, rids + new_tbi, result);
            free (rids); // we copied them, we are done with them
            copy(rhs.rids, rhs.rids + rhs.new_tbi, result + new_tbi);
            size += rhs.size;
            rids = result;
            new_tbi += rhs.new_tbi;
        }
    }
};

/* Create Relation T parallel ctruct */
struct ParallelNonItermediateGreaterFilterT {
    uint64_t * values;
    unsigned * old_rids;
    int filter;
    unsigned new_tbi;
    unsigned * rids;
    uint64_t size;

    /* Initial constructor */
    ParallelNonItermediateGreaterFilterT ( uint64_t * values, unsigned * old_rids, /*unsigned * rids,*/ int filter )
    : values{values}, old_rids{old_rids}, rids{NULL}, filter{filter}, new_tbi(0), size(0)
    {}

    /* Slpitting constructor */
    ParallelNonItermediateGreaterFilterT(ParallelNonItermediateGreaterFilterT & x, split)
    : values{x.values}, old_rids{x.old_rids}, rids{NULL}, filter{x.filter}, new_tbi(0), size(0)
    {}

    /* The function call overloaded operator */
    void operator()(const tbb::blocked_range<size_t>& range) {
        /* Do the write */
        // spin_mutex::scoped_lock lock;
        for (size_t i = range.begin(); i < range.end(); ++i) {
            if (values[i] > filter) {
                // lock.acquire(FilterMutex);
                if (new_tbi == size) {
                    size += 1000;
                    rids = (unsigned*) realloc(rids, size * sizeof(unsigned));
                }
                rids[new_tbi] = i;
                new_tbi++;
                // lock.release();
            }
        }
    }

    /* The function to call on thread join */
    void join( ParallelNonItermediateGreaterFilterT& rhs ) {
        if (rhs.new_tbi) {
            // unsigned * result = new unsigned[new_tbi + rhs.new_tbi];
            unsigned * result = (unsigned*) malloc((new_tbi + rhs.new_tbi) * sizeof(unsigned));
            copy(rids, rids + new_tbi, result);
            free (rids); // we copied them, we are done with them
            copy(rhs.rids, rhs.rids + rhs.new_tbi, result + new_tbi);
            size += rhs.size;
            rids = result;
            new_tbi += rhs.new_tbi;
        }
    }
};

/* Create Relation T parallel ctruct */
struct ParallelItermediateLessFilterT {
    uint64_t * values;
    unsigned * old_rids;
    int filter;
    unsigned new_tbi;
    unsigned * rids;
    uint64_t size;

    /* Initial constructor */
    ParallelItermediateLessFilterT ( uint64_t * values, unsigned * old_rids, /*unsigned * rids,*/ int filter )
    : values{values}, old_rids{old_rids}, rids{NULL}, filter{filter}, new_tbi(0), size(0)
    {}

    /* Slpitting constructor */
    ParallelItermediateLessFilterT(ParallelItermediateLessFilterT & x, split)
    : values{x.values}, old_rids{x.old_rids}, rids{NULL}, filter{x.filter}, new_tbi(0), size(0)
    {}

    /* The function call overloaded operator */
    void operator()(const tbb::blocked_range<size_t>& range) {
        /* Do the write */
        // spin_mutex::scoped_lock lock;
        for (size_t i = range.begin(); i < range.end(); ++i) {
            if (values[old_rids[i]] < filter) {
                // lock.acquire(FilterMutex);
                if (new_tbi == size) {
                    size += 1000;
                    rids = (unsigned*) realloc(rids, size * sizeof(unsigned));
                }
                rids[new_tbi] = old_rids[i];
                new_tbi++;
                // lock.release();
            }
        }
    }

    /* The function to call on thread join */
    void join( ParallelItermediateLessFilterT& rhs ) {
        if (rhs.new_tbi) {
            // unsigned * result = new unsigned[new_tbi + rhs.new_tbi];
            unsigned * result = (unsigned*) malloc((new_tbi + rhs.new_tbi) * sizeof(unsigned));
            copy(rids, rids + new_tbi, result);
            free (rids); // we copied them, we are done with them
            copy(rhs.rids, rhs.rids + rhs.new_tbi, result + new_tbi);
            size += rhs.size;
            rids = result;
            new_tbi += rhs.new_tbi;
        }
    }
};

/* Create Relation T parallel ctruct */
struct ParallelNonItermediateLessFilterT {
    uint64_t * values;
    unsigned * old_rids;
    int filter;
    unsigned new_tbi;
    unsigned * rids;
    uint64_t size;

    /* Initial constructor */
    ParallelNonItermediateLessFilterT ( uint64_t * values, unsigned * old_rids, /*unsigned * rids,*/ int filter )
    : values{values}, old_rids{old_rids}, rids{NULL}, filter{filter}, new_tbi(0), size(0)
    {}

    /* Slpitting constructor */
    ParallelNonItermediateLessFilterT(ParallelNonItermediateLessFilterT & x, split)
    : values{x.values}, old_rids{x.old_rids}, rids{NULL}, filter{x.filter}, new_tbi(0), size(0)
    {}

    /* The function call overloaded operator */
    void operator()(const tbb::blocked_range<size_t>& range) {
        /* Do the write */
        // spin_mutex::scoped_lock lock;
        for (size_t i = range.begin(); i < range.end(); ++i) {
            if (values[i] < filter) {
                // lock.acquire(FilterMutex);
                if (new_tbi == size) {
                    size += 1000;
                    rids = (unsigned*) realloc(rids, size * sizeof(unsigned));
                }
                rids[new_tbi] = i;
                new_tbi++;
                // lock.release();
            }
        }
    }

    /* The function to call on thread join */
    void join( ParallelNonItermediateLessFilterT& rhs ) {
        if (rhs.new_tbi) {
            // unsigned * result = new unsigned[new_tbi + rhs.new_tbi];
            unsigned * result = (unsigned*) malloc((new_tbi + rhs.new_tbi) * sizeof(unsigned));
            copy(rids, rids + new_tbi, result);
            free (rids); // we copied them, we are done with them
            copy(rhs.rids, rhs.rids + rhs.new_tbi, result + new_tbi);
            size += rhs.size;
            rids = result;
            new_tbi += rhs.new_tbi;
        }
    }
};
