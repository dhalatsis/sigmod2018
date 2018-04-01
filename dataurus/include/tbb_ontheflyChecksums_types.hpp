#pragma once
#include "tbb/tbb.h"
#include "tbb/parallel_reduce.h"
#include "tbb/parallel_for.h"
#include "tbb/blocked_range.h"
#include "tbb/partitioner.h"
#include "tuple_buffer.h"
#include "table_t.hpp"

struct checksumST {
    unsigned colId;
    unsigned index;
    unsigned binding;
    uint64_t * values;
};

using namespace tbb;
using namespace std;

/* Struct for prarallel check sum */

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
                //std::cerr << "Index is " << rids[(tups[i].key)*relnum + p.index] << '\n';
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


struct ThreadsParallelT {

    vector<uint64_t> checksums;

    /* Initial constructor */
    ThreadsParallelT(threadresult_t * list, table_t * table_r, table_t * table_s,
                     vector<struct checksumST> * distinctPairs_in_R,
                     vector<struct checksumST> * distinctPairs_in_S
                     )
    :list{list}, table_r{table_r}, table_s{table_s},
    distinctPairs_in_R{distinctPairs_in_R}, distinctPairs_in_S{distinctPairs_in_S}
    {
        checksums.resize((*distinctPairs_in_R).size() + (*distinctPairs_in_S).size(), 0);
    }

    ThreadsParallelT(ThreadsParallelT & x, split)
    :list{x.list}, table_r{x.table_r}, table_s{x.table_s},
    distinctPairs_in_R{x.distinctPairs_in_R}, distinctPairs_in_S{x.distinctPairs_in_S}
    {
        checksums.resize((*distinctPairs_in_R).size() + (*distinctPairs_in_S).size(), 0);
    }

    void join (const ThreadsParallelT & y) {
        for(size_t i = 0; i < y.checksums.size(); i++ ) {
            checksums[i] += y.checksums[i];
        }
    }

    /* The function call overloaded operator */
    void operator()(const tbb::blocked_range<size_t>& range) {

        /* Loop the list of Thread results */
        for (size_t i = range.begin(); i < range.end(); ++i) {
            chainedtuplebuffer_t * cb = (chainedtuplebuffer_t *) list[i].results;

            /* Get the touples form the results */
            tuplebuffer_t * tb = cb->buf;
            uint32_t numbufs = cb->numbufs;

            /* Parallelize first buffer */
            if (!(*distinctPairs_in_R).empty()) {
                CheckSumIntermediateRT crt(tb->tuples, table_r->row_ids, distinctPairs_in_R, table_r->rels_num);
                parallel_reduce(blocked_range<size_t>(0,cb->writepos), crt);

                /* Keep track of the checsums */
                for (size_t i = 0; i < crt.checksums.size(); i++) {
                    checksums[i] += crt.checksums[i];
                }
            }

            if (!(*distinctPairs_in_S).empty()) {
                CheckSumIntermediateST crt(tb->tuples, table_s->row_ids, distinctPairs_in_S, table_s->rels_num);
                parallel_reduce(blocked_range<size_t>(0,cb->writepos), crt);

                /* Keep track of the checsums */
                for (size_t i = 0; i < crt.checksums.size(); i++) {
                    checksums[i + (*distinctPairs_in_R).size()] += crt.checksums[i];
                }
            }

            /* Run the other buffers */
            tb = tb->next;
            for (uint32_t buf_i = 0; buf_i < numbufs - 1; buf_i++) {

                if (!(*distinctPairs_in_R).empty()) {
                    CheckSumIntermediateRT crt(tb->tuples, table_r->row_ids, distinctPairs_in_R, table_r->rels_num);
                    parallel_reduce(blocked_range<size_t>(0,CHAINEDBUFF_NUMTUPLESPERBUF), crt);

                    /* Keep track of the checsums */
                    for (size_t i = 0; i < crt.checksums.size(); i++) {
                        checksums[i] += crt.checksums[i];
                    }
                }

                if (!(*distinctPairs_in_S).empty()) {
                    CheckSumIntermediateST crt(tb->tuples, table_s->row_ids, distinctPairs_in_S, table_s->rels_num);
                    parallel_reduce(blocked_range<size_t>(0,CHAINEDBUFF_NUMTUPLESPERBUF), crt);

                    /* Keep track of the checsums */
                    for (size_t i = 0; i < crt.checksums.size(); i++) {
                        checksums[i + (*distinctPairs_in_R).size()] += crt.checksums[i];
                    }
                }

                /* Go the the next buffer */
                tb = tb->next;
            }
            /* Free cb */
            //chainedtuplebuffer_free(cb);
        }
    }
private:
    threadresult_t * list;
    table_t * table_r;
    table_t * table_s;
    vector<struct checksumST> * distinctPairs_in_R;
    vector<struct checksumST> * distinctPairs_in_S;
};
