#pragma once
#include "tbb/tbb.h"
#include "tbb/parallel_reduce.h"
#include "tbb/parallel_for.h"
#include "tbb/blocked_range.h"
#include "tbb/partitioner.h"

struct checksumST {
    unsigned colId;
    unsigned index;
    unsigned binding;
    uint64_t * values;
};

using namespace tbb;
using namespace std;

/* Struct for praralle check sum */

// Struct for Intermediate S table
struct CheckSumIntermediateRT {
public:
    vector<uint64_t> checksums;

    /* Initial constructor */
    CheckSumIntermediateRT(tuple_t * tups, unsigned * rids, vector<struct checksumST> * distinctPairs, int relnum)
    :tups{tups}, rids{rids}, distinctPairs{distinctPairs}, relnum{relnum}
    {
        /* Create check sums vector */
        checksums.resize((*distinctPairs).size(), 0);
    }

    /* Slpitting constructor */
    CheckSumIntermediateRT(CheckSumIntermediateRT & x, split)
    :tups{x.tups}, rids{x.rids}, distinctPairs{x.distinctPairs}, relnum{x.relnum}
    {
        /*Create check sums vector */
        checksums.resize((*distinctPairs).size(), 0);
    }

    /* How to join the thiefs */
    void join (const CheckSumIntermediateRT & y) {
        for(size_t i = 0; i < y.checksums.size(); i++ ) {
            checksums[i] += y.checksums[i];
        }
    }

    /* The function call overloaded operator */
    void operator()(const tbb::blocked_range<size_t>& range) {
        int idx = 0;
        for (struct checksumST & p: (*distinctPairs)) {
            for (size_t i = range.begin(); i < range.end(); ++i) {
                checksums[idx] += p.values[rids[(tups[i].key)*relnum + p.index]];
            }
            idx++;
        }
    }

private:
    tuple_t  * tups;
    unsigned * rids;
    int relnum;
    vector<struct checksumST> * distinctPairs;
};


// Struct for Intermediate S table
struct CheckSumIntermediateST {
public:
    vector<uint64_t> checksums;

    /* Initial constructor */
    CheckSumIntermediateST(tuple_t * tups, unsigned * rids, vector<struct checksumST> * distinctPairs, int relnum)
    :tups{tups}, rids{rids}, distinctPairs{distinctPairs}, relnum{relnum}
    {
        /* Create check sums vector */
        checksums.resize((*distinctPairs).size(), 0);
    }

    /* Slpitting constructor */
    CheckSumIntermediateST(CheckSumIntermediateST & x, split)
    :tups{x.tups}, rids{x.rids}, distinctPairs{x.distinctPairs}, relnum{x.relnum}
    {
        /*Create check sums vector */
        checksums.resize((*distinctPairs).size(), 0);
    }

    /* How to join the thiefs */
    void join (const CheckSumIntermediateST & y) {
        for(size_t i = 0; i < y.checksums.size(); i++ ) {
            checksums[i] += y.checksums[i];
        }
    }

    /* The function call overloaded operator */
    void operator()(const tbb::blocked_range<size_t>& range) {

        int idx = 0;
        for (struct checksumST & p: (*distinctPairs)) {
            for (size_t i = range.begin(); i < range.end(); ++i) {
                checksums[idx] += p.values[rids[(tups[i].payload)*relnum + p.index]];
            }
            idx++;
        }
    }

private:
    tuple_t  * tups;
    unsigned * rids;
    int relnum;
    vector<struct checksumST> * distinctPairs;
};
/*----------- 4 Structs for parallelizing create table t ----------------*/
struct CheckSumNonIntermediateRT {
public:
    vector<uint64_t> checksums;

    /* Initial constructor */
    CheckSumNonIntermediateRT(tuple_t * tups, vector<struct checksumST> * distinctPairs)
    :tups{tups}, distinctPairs{distinctPairs}
    {
        /* Create check sums vector */
        checksums.resize((*distinctPairs).size(), 0);
    }

    /* Slpitting constructor */
    CheckSumNonIntermediateRT(CheckSumNonIntermediateRT & x, split)
    :tups{x.tups}, distinctPairs{x.distinctPairs}
    {
        /*Create check sums vector */
        checksums.resize((*distinctPairs).size(), 0);
    }

    /* How to join the thiefs */
    void join (const CheckSumNonIntermediateRT & y) {
        for(size_t i = 0; i < y.checksums.size(); i++ ) {
            checksums[i] += y.checksums[i];
        }
    }

    /* The function call overloaded operator */
    void operator()(const tbb::blocked_range<size_t>& range) {

        int idx = 0;
        for (struct checksumST & p: (*distinctPairs)) {
            for (size_t i = range.begin(); i < range.end(); ++i) {
                checksums[idx] += p.values[tups[i].key];
            }
            idx++;
        }
    }

private:
    tuple_t  * tups;
    vector<struct checksumST> * distinctPairs;
};
/*----------- 4 Structs for parallelizing create table t ----------------*/
struct CheckSumNonIntermediateST {
public:
    vector<uint64_t> checksums;

    /* Initial constructor */
    CheckSumNonIntermediateST(tuple_t * tups, vector<struct checksumST> * distinctPairs)
    :tups{tups}, distinctPairs{distinctPairs}
    {
        /* Create check sums vector */
        checksums.resize((*distinctPairs).size(), 0);
    }

    /* Slpitting constructor */
    CheckSumNonIntermediateST(CheckSumNonIntermediateST & x, split)
    :tups{x.tups}, distinctPairs{x.distinctPairs}
    {
        /*Create check sums vector */
        checksums.resize((*distinctPairs).size(), 0);
    }

    /* How to join the thiefs */
    void join (const CheckSumNonIntermediateST & y) {
        for(size_t i = 0; i < y.checksums.size(); i++ ) {
            checksums[i] += y.checksums[i];
        }
    }

    /* The function call overloaded operator */
    void operator()(const tbb::blocked_range<size_t>& range) {

        int idx = 0;
        for (struct checksumST & p: (*distinctPairs)) {
            for (size_t i = range.begin(); i < range.end(); ++i) {
                checksums[idx] += p.values[tups[i].payload];
            }
            idx++;
        }
    }

private:
    tuple_t  * tups;
    vector<struct checksumST> * distinctPairs;
};
