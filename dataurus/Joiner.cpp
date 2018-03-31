#include <cassert>
#include <iostream>
#include <string>
#include <unordered_map>
#include <algorithm>
#include <functional>
#include <array>
#include <utility>
#include <vector>
#include <map>
#include <set>
#include <pthread.h>

#include "generator.h"
#include "QueryPlan.hpp"
#include "Joiner.hpp"
#include "tbb_parallel_types.hpp"

using namespace tbb;
using namespace std;

//#define TIME_DETAILS
//#include <sstream>
//string timeDetStr = "";

bool done_testing = false;

/* Timing variables */
extern double timeSelfJoin;
extern double timeSelectFilter;
extern double timeEqualFilter;
extern double timeLessFilter;
extern double timeGreaterFilter;
extern double timeIntermediateFilters;
extern double timeNonIntermediateFilters;

double timeAddColumn = 0;
double timeCreateTable = 0;
double timeTreegen = 0;
double timeCheckSum = 0;
double timeRadixJoin = 0;
double timeCreateRelationT = 0;
double timeCreateRelI = 0;
double timeCreateRelNonI = 0;
double timeCreateTableT = 0;
double timeCheckSumsOnTheFly = 0;
double timeCTPrepear =0;
double timeCT1bucket = 0;
double timeCTMoreBuckets = 0;
double timeExecute = 0;
double timePreparation = 0;
double timeCleanQuery = 0;
double timeToLoop = 0;


int cleanQuery(QueryInfo &info) {

#ifdef time
    struct timeval start;
    gettimeofday(&start, NULL);
#endif

    /* Remove weak filters */
    int changed = 0;

    map<SelectInfo, FilterInfo> filter_mapG;
    map<SelectInfo, FilterInfo> filter_mapL;
    set<FilterInfo> filters;

    for (auto filter: info.filters) {
        if (filter.comparison == '<') {
            if ((filter_mapL.find(filter.filterColumn) == filter_mapL.end())
                    || (filter_mapL[filter.filterColumn].constant > filter.constant)) {
                filter_mapL[filter.filterColumn] = filter;
            }

        }
        else if (filter.comparison == '>'){
            if ((filter_mapG.find(filter.filterColumn) == filter_mapG.end())
                    || (filter_mapG[filter.filterColumn].constant < filter.constant)) {
                filter_mapG[filter.filterColumn] = filter;
            }
        }
        else {
            filters.insert(filter);
        }
    }

    info.filters.clear();
    vector<FilterInfo> newfilters;
    for (auto filter: filters) {
        info.filters.push_back(filter);
    }

    for (std::map<SelectInfo,FilterInfo>::iterator it=filter_mapG.begin(); it!=filter_mapG.end(); ++it) {
        info.filters.push_back(it->second);
    }

    for (std::map<SelectInfo,FilterInfo>::iterator it=filter_mapL.begin(); it!=filter_mapL.end(); ++it) {
        info.filters.push_back(it->second);
    }

    /* Remove duplicate predicates */
    changed = 0;
    set <PredicateInfo> pred_set;
    for (auto pred: info.predicates) {
        if (!(pred.left < pred.right)) {
            SelectInfo tmp = pred.left;
            pred.left = pred.right;
            pred.right = tmp;
        //    cerr << "swapped" << endl;
        }

        if (pred_set.find(pred) != pred_set.end()) {
            changed = 1;
            continue;
        }
        pred_set.insert(pred);
    }

    if (changed == 0) {
        return 0;
    }

    info.predicates.clear();
    for (auto pred: pred_set) {
        info.predicates.push_back(pred);
    }

#ifdef time
    struct timeval end;
    gettimeofday(&end, NULL);
    timeCleanQuery += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
#endif

    return 0;
}

/* ================================ */
/* Table_t <=> Relation_t fuctnions */
/* ================================ */
relation_t * Joiner::CreateRelationT(table_t * table, SelectInfo &sel_info) {

    /* Create a new column_t for table */
    unsigned * row_ids = table->row_ids;

    /* Get the relation from joiner */
    Relation &rel = getRelation(sel_info.relId);
    uint64_t * values = rel.columns.at(sel_info.colId);

    /* Create the relation_t */
    relation_t * new_relation = gen_rel(table->tups_num);

    // Get the range for the threds chinking
    size_t range = THREAD_NUM_1CPU; // + THREAD_NUM_2CPU;

    if (table->intermediate_res) {
        #ifdef time
        struct timeval start;
        gettimeofday(&start, NULL);
        #endif

        unsigned table_index = -1;
        unsigned relation_binding = sel_info.binding;

        /* Get the right index from the relation id table */
        unordered_map<unsigned, unsigned>::iterator itr = table->relations_bindings.find(relation_binding);
        if (itr != table->relations_bindings.end())
            table_index = itr->second;
        else
            std::cerr << "At AddColumnToTableT, Id not matchin with intermediate result vectors for " << relation_binding <<'\n';

        /* Initialize relation */
        uint32_t size    = table->tups_num;
        uint32_t rel_num = table->rels_num;
        struct interRel_arg a[range];
        for (size_t i = 0; i < range; i++) {
            a[i].low   = (i < size % range) ? i * (size / range) + i : i * (size / range) + size % range;
            a[i].high  = (i < size % range) ? a[i].low + size / range + 1 :  a[i].low + size / range;
            a[i].values = values;
            a[i].tups = new_relation->tuples;
            a[i].rids = row_ids;
            a[i].rel_num = rel_num;
            a[i].table_index = table_index;
            job_scheduler1.Schedule(new JobCreateInterRel(a[i]));
        }
        job_scheduler1.Barrier();

        //RelationIntermediateCT rct( new_relation->tuples, values, row_ids, rel_num, table_index );
        //parallel_for(blocked_range<size_t>(0,size), rct);

        #ifdef time
        struct timeval end;
        gettimeofday(&end, NULL);
        timeCreateRelI += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
        #endif

    }
    else {
        #ifdef time
        struct timeval start, end;
        gettimeofday(&start, NULL);
        #endif

        /* Initialize relation */
        uint32_t size = table->tups_num;
        struct noninterRel_arg a[range];
        for (size_t i = 0; i < range; i++) {
            a[i].low   = (i < size % range) ? i * (size / range) + i : i * (size / range) + size % range;
            a[i].high  = (i < size % range) ? a[i].low + size / range + 1 :  a[i].low + size / range;
            a[i].values = values;
            a[i].tups = new_relation->tuples;
            job_scheduler1.Schedule(new JobCreateNonInterRel(a[i]));
        }
        job_scheduler1.Barrier();

        // RelationNonIntermediateCT rct( new_relation->tuples, values );
        // parallel_for(blocked_range<size_t>(0,size), rct);

        #ifdef time
        gettimeofday(&end, NULL);
        timeCreateRelNonI += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
        #endif
    }

    return new_relation;
}


relation_t * Joiner::CreateRowRelationT(uint64_t * column, unsigned size) {

    /* Create the relation_t */
    relation_t * new_relation = gen_rel(size);

    // Get the range for the threds chinking
    size_t range = THREAD_NUM_1CPU; // + THREAD_NUM_2CPU;

    /* Initialize relation */
    struct noninterRel_arg a[range];
    for (size_t i = 0; i < range; i++) {
        a[i].low   = (i < size % range) ? i * (size / range) + i : i * (size / range) + size % range;
        a[i].high  = (i < size % range) ? a[i].low + size / range + 1 :  a[i].low + size / range;
        a[i].values = column;
        a[i].tups = new_relation->tuples;
        job_scheduler1.Schedule(new JobCreateNonInterRel(a[i]));
    }
    job_scheduler1.Barrier();

    return new_relation;
}

void Joiner::CheckSumOnTheFly(result_t * result, table_t * table_r, table_t * table_s, columnInfoMap & cmap, std::vector<SelectInfo> selections, string & result_str) {
#ifdef time
    struct timeval start;
    gettimeofday(&start, NULL);
#endif

        /* Crete a vector for the pairs Column, Index in relationR/S */
        vector<struct checksumST> distinctPairs_in_R;
        vector<struct checksumST> distinctPairs_in_S;
        struct checksumST st;

        /* take the distinct columns in a vector */
        unordered_map<unsigned, unsigned>::iterator itr;
        unsigned index = 0;
        for (columnInfoMap::iterator it=cmap.begin(); it != cmap.end(); it++) {
            index = -1;
            itr = table_r->relations_bindings.find(it->first.binding);
            if (itr != table_r->relations_bindings.end() ) {
                st.colId = it->first.colId;
                st.binding = it->first.binding;
                st.index = itr->second;
                st.values = getRelation(it->first.relId).columns[st.colId];
                distinctPairs_in_R.push_back(st);
            }
            else {
                itr = table_s->relations_bindings.find(it->first.binding);
                (itr != table_s->relations_bindings.end()) ? (index = itr->second) : (index = -1);
                st.colId = it->first.colId;
                st.binding = it->first.binding;
                st.index = itr->second;
                st.values = getRelation(it->first.relId).columns[st.colId];
                distinctPairs_in_S.push_back(st);
            }

        }

        // std::cerr << "Pairs in R (" << distinctPairs_in_R.empty() << "):";
        // for (size_t i = 0; i < distinctPairs_in_R.size(); i++) {
        //     std::cerr << distinctPairs_in_R[i].colId << "." << distinctPairs_in_R[i].index << ' ';
        // }
        // std::cerr << '\n';
        // std::cerr << "Pairs in S (" << distinctPairs_in_S.empty() << ") :";
        // for (size_t i = 0; i < distinctPairs_in_S.size(); i++) {
        //     std::cerr << distinctPairs_in_S[i].colId << "." << distinctPairs_in_S[i].index << ' ';
        // }
        // std::cerr << '\n';

        vector<uint64_t> sum(distinctPairs_in_R.size() + distinctPairs_in_S.size(), 0);
        if (table_r->intermediate_res && table_s->intermediate_res) {
            for (int th = 0; th < THREAD_NUM; th++) {
                chainedtuplebuffer_t * cb = (chainedtuplebuffer_t *) result->resultlist[th].results;

                /* Get the touples form the results */
                tuplebuffer_t * tb = cb->buf;
                uint32_t numbufs = cb->numbufs;

                /* Parallelize first buffer */
                if (!distinctPairs_in_R.empty()) {
                    CheckSumIntermediateRT crt(tb->tuples, table_r->row_ids, &distinctPairs_in_R, table_r->rels_num);
                    parallel_reduce(blocked_range<size_t>(0,cb->writepos), crt);

                    /* Keep track of the checsums */
                    for (size_t i = 0; i < crt.checksums.size(); i++) {
                        sum[i] += crt.checksums[i];
                    }
                }

                if (!distinctPairs_in_S.empty()) {
                    CheckSumIntermediateST crt(tb->tuples, table_s->row_ids, &distinctPairs_in_S, table_s->rels_num);
                    parallel_reduce(blocked_range<size_t>(0,cb->writepos), crt);

                    /* Keep track of the checsums */
                    for (size_t i = 0; i < crt.checksums.size(); i++) {
                        sum[i + distinctPairs_in_R.size()] += crt.checksums[i];
                    }
                }

                /* Run the other buffers */
                affinity_partitioner ap;
                tb = tb->next;
                for (uint32_t buf_i = 0; buf_i < numbufs - 1; buf_i++) {

                    if (!distinctPairs_in_R.empty()) {
                        CheckSumIntermediateRT crt(tb->tuples, table_r->row_ids, &distinctPairs_in_R, table_r->rels_num);
                        parallel_reduce(blocked_range<size_t>(0,CHAINEDBUFF_NUMTUPLESPERBUF), crt, ap);

                        /* Keep track of the checsums */
                        for (size_t i = 0; i < crt.checksums.size(); i++) {
                            sum[i] += crt.checksums[i];
                        }
                    }

                    if (!distinctPairs_in_S.empty()) {
                        CheckSumIntermediateST crt(tb->tuples, table_s->row_ids, &distinctPairs_in_S, table_s->rels_num);
                        parallel_reduce(blocked_range<size_t>(0,CHAINEDBUFF_NUMTUPLESPERBUF), crt, ap);

                        /* Keep track of the checsums */
                        for (size_t i = 0; i < crt.checksums.size(); i++) {
                            sum[i + distinctPairs_in_R.size()] += crt.checksums[i];
                        }
                    }

                    /* Go the the next buffer */
                    tb = tb->next;
                }
                /* Free cb */
                //chainedtuplebuffer_free(cb);
            }

            // ThreadsParallelT tt(result->resultlist, table_r, table_s, &distinctPairs_in_R, &distinctPairs_in_S);
            // parallel_reduce(blocked_range<size_t>(0, THREAD_NUM), tt);
            // for (size_t i = 0; i < sum.size(); i++) {
            //     sum[i] = tt.checksums[i];
            // }
        }
        else if (table_r->intermediate_res) {
            // std::cerr << "HERE" << '\n';
            for (int th = 0; th < THREAD_NUM; th++) {
                chainedtuplebuffer_t * cb = (chainedtuplebuffer_t *) result->resultlist[th].results;

                /* Get the touples form the results */
                tuplebuffer_t * tb = cb->buf;
                uint32_t numbufs = cb->numbufs;

                /* Parallelize first buffer */
                if (!distinctPairs_in_R.empty()) {
                    CheckSumIntermediateRT crt(tb->tuples, table_r->row_ids, &distinctPairs_in_R, table_r->rels_num);
                    parallel_reduce(blocked_range<size_t>(0,cb->writepos), crt);

                    /* Keep track of the checsums */
                    for (size_t i = 0; i < crt.checksums.size(); i++) {
                        sum[i] += crt.checksums[i];
                    }
                }

                if (!distinctPairs_in_S.empty()) {
                    CheckSumNonIntermediateST crt(tb->tuples, &distinctPairs_in_S);
                    parallel_reduce(blocked_range<size_t>(0,cb->writepos), crt);

                    /* Keep track of the checsums */
                    for (size_t i = 0; i < crt.checksums.size(); i++) {
                        sum[i + distinctPairs_in_R.size()] += crt.checksums[i];
                    }
                }

                /* Run the other buffers */
                affinity_partitioner ap;
                tb = tb->next;
                for (uint32_t buf_i = 0; buf_i < numbufs - 1; buf_i++) {

                    if (!distinctPairs_in_R.empty()) {
                        CheckSumIntermediateRT crt(tb->tuples, table_r->row_ids, &distinctPairs_in_R, table_r->rels_num);
                        parallel_reduce(blocked_range<size_t>(0,CHAINEDBUFF_NUMTUPLESPERBUF), crt, ap);

                        /* Keep track of the checsums */
                        for (size_t i = 0; i < crt.checksums.size(); i++) {
                            sum[i] += crt.checksums[i];
                        }
                    }

                    if (!distinctPairs_in_S.empty()) {
                        CheckSumNonIntermediateST crt(tb->tuples, &distinctPairs_in_S);
                        parallel_reduce(blocked_range<size_t>(0,CHAINEDBUFF_NUMTUPLESPERBUF), crt, ap);

                        for (size_t i = 0; i < crt.checksums.size(); i++) {
                            sum[i + distinctPairs_in_R.size()] += crt.checksums[i];
                        }
                    }

                    /* Go the the next buffer */
                    tb = tb->next;
                }
                /* Free cb */
                //chainedtuplebuffer_free(cb);
            }
        }
        else if (table_s->intermediate_res) {
            for (int th = 0; th < THREAD_NUM; th++) {
                chainedtuplebuffer_t * cb = (chainedtuplebuffer_t *) result->resultlist[th].results;

                /* Get the touples form the results */
                tuplebuffer_t * tb = cb->buf;
                uint32_t numbufs = cb->numbufs;

                /* Parallelize first buffer */
                if (!distinctPairs_in_R.empty()) {
                    CheckSumNonIntermediateRT crt(tb->tuples, &distinctPairs_in_R);
                    parallel_reduce(blocked_range<size_t>(0,cb->writepos), crt);

                    /* Keep track of the checsums */
                    for (size_t i = 0; i < crt.checksums.size(); i++) {
                        sum[i] += crt.checksums[i];
                    }
                }

                if (!distinctPairs_in_S.empty()) {
                    CheckSumIntermediateST crt(tb->tuples, table_s->row_ids, &distinctPairs_in_S, table_s->rels_num);
                    parallel_reduce(blocked_range<size_t>(0,cb->writepos), crt);

                    /* Keep track of the checsums */
                    for (size_t i = 0; i < crt.checksums.size(); i++) {
                        sum[i + distinctPairs_in_R.size()] += crt.checksums[i];
                    }
                }

                /* Run the other buffers */
                affinity_partitioner ap;
                tb = tb->next;
                for (uint32_t buf_i = 0; buf_i < numbufs - 1; buf_i++) {

                    if (!distinctPairs_in_R.empty()) {
                        CheckSumNonIntermediateRT crt(tb->tuples, &distinctPairs_in_R);
                        parallel_reduce(blocked_range<size_t>(0,CHAINEDBUFF_NUMTUPLESPERBUF), crt, ap);

                        /* keep track of the sum */
                        for (size_t i = 0; i < crt.checksums.size(); i++) {
                            sum[i] += crt.checksums[i];
                        }
                    }

                    if (!distinctPairs_in_S.empty()) {
                        CheckSumIntermediateST crt(tb->tuples, table_s->row_ids, &distinctPairs_in_S, table_s->rels_num);
                        parallel_reduce(blocked_range<size_t>(0,CHAINEDBUFF_NUMTUPLESPERBUF), crt, ap);

                        /* Keep track of the checsums */
                        for (size_t i = 0; i < crt.checksums.size(); i++) {
                            sum[i + distinctPairs_in_R.size()] += crt.checksums[i];
                        }
                    }

                    /* Go the the next buffer */
                    tb = tb->next;
                }
                /* Free cb */
                //chainedtuplebuffer_free(cb);
            }
        }
        else {
            for (int th = 0; th < THREAD_NUM; th++) {
                chainedtuplebuffer_t * cb = (chainedtuplebuffer_t *) result->resultlist[th].results;

                /* Get the touples form the results */
                tuplebuffer_t * tb = cb->buf;
                uint32_t numbufs = cb->numbufs;

                /* Parallelize first buffer */
                if (!distinctPairs_in_R.empty()) {
                    CheckSumNonIntermediateRT crt(tb->tuples, &distinctPairs_in_R);
                    parallel_reduce(blocked_range<size_t>(0,cb->writepos), crt);

                    for (size_t i = 0; i < crt.checksums.size(); i++) {
                        sum[i] += crt.checksums[i];
                    }
                }

                if (!distinctPairs_in_S.empty()) {
                    CheckSumNonIntermediateST crt(tb->tuples, &distinctPairs_in_S);
                    parallel_reduce(blocked_range<size_t>(0,cb->writepos), crt);

                    /* Keep track of the checsums */
                    for (size_t i = 0; i < crt.checksums.size(); i++) {
                        sum[i + distinctPairs_in_R.size()] += crt.checksums[i];
                    }
                }

                /* Run the other buffers */
                affinity_partitioner ap;
                tb = tb->next;
                for (uint32_t buf_i = 0; buf_i < numbufs - 1; buf_i++) {

                    if (!distinctPairs_in_R.empty()) {
                        CheckSumNonIntermediateRT crt(tb->tuples, &distinctPairs_in_R);
                        parallel_reduce(blocked_range<size_t>(0,CHAINEDBUFF_NUMTUPLESPERBUF), crt, ap);

                        /* Keep track of the checsums */
                        for (size_t i = 0; i < crt.checksums.size(); i++) {
                            sum[i] += crt.checksums[i];
                        }
                    }

                    if (!distinctPairs_in_S.empty()) {
                        CheckSumNonIntermediateST crt(tb->tuples, &distinctPairs_in_S);
                        parallel_reduce(blocked_range<size_t>(0,CHAINEDBUFF_NUMTUPLESPERBUF), crt, ap);

                        /* Keep track of the checsums */
                        for (size_t i = 0; i < crt.checksums.size(); i++) {
                            sum[i + distinctPairs_in_R.size()] += crt.checksums[i];
                        }
                    }

                    /* Go the the next buffer */
                    tb = tb->next;
                }
                /* Free cb */
                //chainedtuplebuffer_free(cb);
            }
        }

        /* Construct the checksums in the right way */
        bool found = false;
        for (size_t i = 0; i < selections.size(); i++) {

            // Check if checksum is cached
            for (size_t j = 0; j < distinctPairs_in_R.size(); j++) {
                if (selections[i].colId == distinctPairs_in_R[j].colId
                    && selections[i].binding == distinctPairs_in_R[j].binding)
                {
                    (sum[j] == 0) ? result_str += "NULL" : result_str += to_string(sum[j]);
                    found = true;
                    break;
                }
            }

            /* Search in the other */
            for (size_t j = 0; j < distinctPairs_in_S.size() && found != true; j++) {
                if (selections[i].colId == distinctPairs_in_S[j].colId
                    && selections[i].binding == distinctPairs_in_S[j].binding)
                {
                    (sum[j] == 0) ? result_str += "NULL" : result_str += to_string(sum[j +  distinctPairs_in_R.size()]);
                    break;
                }
            }

            /* Flag for the next loop */
            found = false;

            // Create the write check sum
            if (i != selections.size() - 1) {
                result_str +=  " ";
            }
        }

        #ifdef time
        struct timeval end;
        gettimeofday(&end, NULL);
        timeCheckSumsOnTheFly += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
        #endif
}

table_t * Joiner::CreateTableT(result_t * result, table_t * table_r, table_t * table_s, columnInfoMap & cmap) {

#ifdef time
    struct timeval start;
    gettimeofday(&start, NULL);
#endif

    /* Cut the unused relations */
    unordered_map<unsigned, unsigned>::iterator itr;
    vector<unsigned> help_v_r;
    vector<unsigned> help_v_s;
    vector<unordered_map<unsigned, unsigned>::iterator> victimized_r;
    vector<unordered_map<unsigned, unsigned>::iterator> victimized_s;
    bool victimize = true;
    int  index     = -1;
    int left_removed = 0, right_removed = 0;

    for (itr = table_r->relations_bindings.begin(); itr != table_r->relations_bindings.end(); itr++) {
        victimize = true;
        for (columnInfoMap::iterator it=cmap.begin(); it != cmap.end(); it++) {
            if (it->first.binding == itr->first) {
                victimize = false;
                help_v_r.push_back(itr->second);
                break;
            }
        }
        if (victimize) {
            victimized_r.push_back(itr);
            left_removed++;
        }
    }

    // const unsigned size1 = table_r->relations_bindings.size();
    // ParallelVictimizeT vic1( table_r, cmap );
    // parallel_reduce(blocked_range<size_t>(0,size1,GRAINSIZE), vic1);
    // help_v_r.insert( help_v_r.end(), vic1.help_v.begin(), vic1.help_v.end() );
    // victimized_r.insert( victimized_r.end(), vic1.victimized.begin(), vic1.victimized.end() );
    // left_removed += vic1.removed;

    for (itr = table_s->relations_bindings.begin(); itr != table_s->relations_bindings.end(); itr++) {
        victimize = true;
        for (columnInfoMap::iterator it=cmap.begin(); it != cmap.end(); it++) {
            if (it->first.binding == itr->first) {
                victimize = false;
                help_v_s.push_back(itr->second);
                break;
            }
        }
        if (victimize) {
            victimized_s.push_back(itr);
            right_removed++;
        }
    }

    // const unsigned size2 = table_s->relations_bindings.size();
    // ParallelVictimizeT vic2( table_s, cmap );
    // parallel_reduce(blocked_range<size_t>(0,size2,GRAINSIZE), vic2);
    // help_v_s.insert( help_v_s.end(), vic2.help_v.begin(), vic2.help_v.end() );
    // victimized_s.insert( victimized_s.end(), vic2.victimized.begin(), vic2.victimized.end() );
    // right_removed += vic2.removed;

    /* Erase Victimized */
    for (size_t i = 0; i < victimized_r.size(); i++) {
        table_r->relations_bindings.erase(victimized_r[i]);
    }
    for (size_t i = 0; i < victimized_s.size(); i++) {
        table_s->relations_bindings.erase(victimized_s[i]);
    }

    /* sort the un-victimized helping arrays */
    std::sort(help_v_r.begin(), help_v_r.end());
    std::sort(help_v_s.begin(), help_v_s.end());

    /* The num of relations for the two tables */
    const unsigned relnum_r = table_r->relations_bindings.size();
    const unsigned relnum_s = table_s->relations_bindings.size();

    /* Create - Initialize the new table_t */
    uint32_t num_relations = table_r->relations_bindings.size() + table_s->relations_bindings.size();
    table_t * new_table = new table_t;
    new_table->intermediate_res = true;
    new_table->column_j = new column_t;
    new_table->tups_num = result->totalresults;
    new_table->rels_num = num_relations;
    new_table->row_ids = (unsigned *) malloc(sizeof(unsigned) * num_relations * result->totalresults);

    /* Create the new maping vector */
    for (itr = table_r->relations_bindings.begin(); itr != table_r->relations_bindings.end(); itr++) {
        (itr->second >= left_removed)
        ? new_table->relations_bindings.insert(make_pair(itr->first, itr->second - left_removed))
        : new_table->relations_bindings.insert(make_pair(itr->first, itr->second));
    }

    for (itr = table_s->relations_bindings.begin(); itr != table_s->relations_bindings.end(); itr++) {
        (itr->second >= right_removed)
        ? new_table->relations_bindings.insert(make_pair(itr->first, relnum_r + itr->second - right_removed))
        : new_table->relations_bindings.insert(make_pair(itr->first, relnum_r + itr->second));
    }

    /* PRINTS
    std::cerr << endl << "New mapping ";
    for (itr = new_table->relations_bindings.begin(); itr != new_table->relations_bindings.end(); itr++) {
        std::cerr << itr->first << "." << itr->second << " ";
    }
    std::cerr << '\n';


    /* PRINTS
    std::cerr << endl << "New mapping ";
    for (itr = new_table->relations_bindings.begin(); itr != new_table->relations_bindings.end(); itr++) {
        std::cerr << itr->first << "." << itr->second << " ";
    }
    std::cerr << '\n';
    std::cerr << "Helper v R ";
    for (size_t i = 0; i < help_v_r.size(); i++) {
        cerr << help_v_r[i] << " ";
    }
    std::cerr << endl << "Helper v S ";
    for (size_t i = 0; i < help_v_s.size(); i++) {
        cerr << help_v_s[i] << " ";
    }
    std::cerr << '\n';
    */

    /* Get the 3 row_ids matrixes in referances */
    unsigned * rids_res = new_table->row_ids;
    unsigned * rids_r   = table_r->row_ids;
    unsigned * rids_s   = table_s->row_ids;

    /* Get the chained buffer */
    unordered_map<unsigned, unsigned> & relB_r = table_r->relations_bindings;
    unordered_map<unsigned, unsigned> & relB_s = table_s->relations_bindings;
    const unsigned old_relnum_r = table_r->rels_num;
    const unsigned old_relnum_s = table_s->rels_num;

#ifdef time
    struct timeval end;
    gettimeofday(&end, NULL);
    timeCTPrepear += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
#endif


    uint32_t idx = 0;  // POints to the right index on the res
    uint32_t tup_i;

    // unsigned * a = new unsigned[1];
    // for (int th = 0; th < THREAD_NUM; th++) {
    //     chainedtuplebuffer_t * cb = (chainedtuplebuffer_t *) result->resultlist[th].results;
    //
    //     /* Get the touples form the results */
    //     tuplebuffer_t * tb = cb->buf;
    //     uint32_t numbufs = cb->numbufs;
    //     uint32_t row_i;
    //
    //     /* Parallelize first buffer */
    //     for (size_t i = 0; i < cb->writepos; i++) {
    //         row_i = tb->tuples[i].key;
    //         for (size_t j = 0; j < help_v_r.size(); j++) {
    //             *a = row_i;
    //         }
    //
    //         row_i = tb->tuples[i].payload;
    //         for (size_t j = 0; j < help_v_s.size(); j++) {
    //             *a = row_i;
    //         }
    //     }
    //
    //     /* Run the other buffers */
    //     tb = tb->next;
    //     for (uint32_t buf_i = 0; buf_i < numbufs - 1; buf_i++) {
    //         for (size_t i = 0; i < CHAINEDBUFF_NUMTUPLESPERBUF; i++) {
    //             row_i = tb->tuples[i].key;
    //             for (size_t j = 0; j < help_v_r.size(); j++) {
    //                 *a = row_i;
    //             }
    //
    //             row_i = tb->tuples[i].payload;
    //             for (size_t j = 0; j < help_v_s.size(); j++) {
    //                 *a = row_i;
    //             }
    //         }
    //         tb = tb->next;
    //     }
    // }
    // #ifdef time
    //     gettimeofday(&end, NULL);
    //     timeCTPrepear += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
    // #endif

    /* Depending on tables choose what to pass */
    if (table_r->intermediate_res && table_s->intermediate_res) {

        for (int th = 0; th < THREAD_NUM; th++) {
            chainedtuplebuffer_t * cb = (chainedtuplebuffer_t *) result->resultlist[th].results;

            /* Get the touples form the results */
            tuplebuffer_t * tb = cb->buf;
            uint32_t numbufs = cb->numbufs;
            uint32_t row_i;

            #ifdef time
            gettimeofday(&start, NULL);
            #endif

            /* Parallelize first buffer */
            TableAllIntermediateCT tct
            (
                tb->tuples,
                rids_res, rids_r, rids_s,
                &help_v_r, &help_v_s,
                idx, old_relnum_r, old_relnum_s, num_relations
            );
            parallel_for(blocked_range<size_t>(0,cb->writepos), tct);

            #ifdef time
            gettimeofday(&end, NULL);
            timeCT1bucket += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
            gettimeofday(&start, NULL);
            #endif

            /* Run the other buffers */
            tb = tb->next;
            idx += cb->writepos;
            for (uint32_t buf_i = 0; buf_i < numbufs - 1; buf_i++) {
                TableAllIntermediateCT tct
                (
                    tb->tuples,
                    rids_res, rids_r, rids_s,
                    &help_v_r, &help_v_s,
                    idx, old_relnum_r, old_relnum_s, num_relations
                );
                parallel_for(blocked_range<size_t>(0,CHAINEDBUFF_NUMTUPLESPERBUF), tct);

                /* Go the the next buffer */
                idx += CHAINEDBUFF_NUMTUPLESPERBUF;
                tb = tb->next;
            }
            /* Free cb */
            chainedtuplebuffer_free(cb);

            #ifdef time
            gettimeofday(&end, NULL);
            timeCTMoreBuckets += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
            #endif
        }
    }
    else if (table_r->intermediate_res) {

        for (int th = 0; th < THREAD_NUM; th++) {
            chainedtuplebuffer_t * cb = (chainedtuplebuffer_t *) result->resultlist[th].results;

            /* Get the touples form the results */
            tuplebuffer_t * tb = cb->buf;
            uint32_t numbufs = cb->numbufs;
            uint32_t row_i;

            #ifdef time
            gettimeofday(&start, NULL);
            #endif

            /* Parallelize first buffer */
            TableRIntermediateCT tct
            (
                tb->tuples,
                rids_res, rids_r, rids_s,
                &help_v_r, &help_v_s,
                idx, old_relnum_r, old_relnum_s, num_relations
            );
            parallel_for(blocked_range<size_t>(0,cb->writepos), tct);

            #ifdef time
            gettimeofday(&end, NULL);
            timeCT1bucket += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
            gettimeofday(&start, NULL);
            #endif

            /* Run the other buffers */
            tb = tb->next;
            idx += cb->writepos;
            for (uint32_t buf_i = 0; buf_i < numbufs - 1; buf_i++) {
                TableRIntermediateCT tct
                (
                    tb->tuples,
                    rids_res, rids_r, rids_s,
                    &help_v_r, &help_v_s,
                    idx, old_relnum_r, old_relnum_s, num_relations
                );
                parallel_for(blocked_range<size_t>(0,CHAINEDBUFF_NUMTUPLESPERBUF), tct);

                /* Go the the next buffer */
                idx += CHAINEDBUFF_NUMTUPLESPERBUF;
                tb = tb->next;
            }
            /* Free cb */
            chainedtuplebuffer_free(cb);

            #ifdef time
            gettimeofday(&end, NULL);
            timeCTMoreBuckets += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
            #endif
        }

    }
    else if (table_s->intermediate_res) {

        for (int th = 0; th < THREAD_NUM; th++) {
            chainedtuplebuffer_t * cb = (chainedtuplebuffer_t *) result->resultlist[th].results;

            /* Get the touples form the results */
            tuplebuffer_t * tb = cb->buf;
            uint32_t numbufs = cb->numbufs;
            uint32_t row_i;

            #ifdef time
            gettimeofday(&start, NULL);
            #endif

            /* Parallelize first buffer */
            TableSIntermediateCT tct
            (
                tb->tuples,
                rids_res, rids_r, rids_s,
                &help_v_r, &help_v_s,
                idx, old_relnum_r, old_relnum_s, num_relations
            );
            parallel_for(blocked_range<size_t>(0,cb->writepos), tct);

            #ifdef time
            gettimeofday(&end, NULL);
            timeCT1bucket += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
            gettimeofday(&start, NULL);
            #endif

            /* Run the other buffers */
            tb = tb->next;
            idx += cb->writepos;
            for (uint32_t buf_i = 0; buf_i < numbufs - 1; buf_i++) {
                TableSIntermediateCT tct
                (
                    tb->tuples,
                    rids_res, rids_r, rids_s,
                    &help_v_r, &help_v_s,
                    idx, old_relnum_r, old_relnum_s, num_relations
                );
                parallel_for(blocked_range<size_t>(0,CHAINEDBUFF_NUMTUPLESPERBUF), tct);
                /* Go the the next buffer */
                idx += CHAINEDBUFF_NUMTUPLESPERBUF;
                tb = tb->next;
            }
            /* Free cb */
            chainedtuplebuffer_free(cb);
        }

        #ifdef time
        gettimeofday(&end, NULL);
        timeCTMoreBuckets += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
        #endif
    }
    else {
        for (int th = 0; th < THREAD_NUM; th++) {
            chainedtuplebuffer_t * cb = (chainedtuplebuffer_t *) result->resultlist[th].results;

            /* Get the touples form the results */
            tuplebuffer_t * tb = cb->buf;
            uint32_t numbufs = cb->numbufs;
            uint32_t row_i;

            #ifdef time
            gettimeofday(&start, NULL);
            #endif

            /* Parallelize first buffer */
            TableNoneIntermediateCT tct
            (
                tb->tuples,
                rids_res, rids_r, rids_s,
                &help_v_r, &help_v_s,
                idx, old_relnum_r, old_relnum_s, num_relations
            );
            parallel_for(blocked_range<size_t>(0,cb->writepos), tct);

            #ifdef time
            gettimeofday(&end, NULL);
            timeCT1bucket += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
            gettimeofday(&start, NULL);
            #endif

            /* Run the other buffers */
            tb = tb->next;
            idx += cb->writepos;
            for (uint32_t buf_i = 0; buf_i < numbufs - 1; buf_i++) {
                TableNoneIntermediateCT tct
                (
                    tb->tuples,
                    rids_res, rids_r, rids_s,
                    &help_v_r, &help_v_s,
                    idx, old_relnum_r, old_relnum_s, num_relations
                );
                parallel_for(blocked_range<size_t>(0,CHAINEDBUFF_NUMTUPLESPERBUF), tct);
                /* Go the the next buffer */
                idx += CHAINEDBUFF_NUMTUPLESPERBUF;
                tb = tb->next;
            }
            /* Free cb */
            chainedtuplebuffer_free(cb);
        }

        #ifdef time
        gettimeofday(&end, NULL);
        timeCTMoreBuckets += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
        #endif
    }

    return new_table;
}

/* =================================== */

/* +---------------------+
   |The joiner functions |
   +---------------------+ */


   void Joiner::AddColumnToTableT(SelectInfo &sel_info, table_t *table) {

   #ifdef time
       struct timeval start;
       gettimeofday(&start, NULL);
   #endif

       /* Create a new column_t for table */
       column_t &column = *table->column_j;

       /* Get the relation from joiner */
       Relation &rel = getRelation(sel_info.relId);
       column.size   = rel.size;
       column.values = rel.columns.at(sel_info.colId);
       column.id     = sel_info.colId;
       column.table_index = -1;
       unsigned relation_binding = sel_info.binding;

       /* Get the right index from the relation id table */
       unordered_map<unsigned, unsigned>::iterator itr = table->relations_bindings.find(relation_binding);
       if (itr != table->relations_bindings.end())
           column.table_index = itr->second;
       else
           std::cerr << "At AddColumnToTableT, Id not matchin with intermediate result vectors for " << relation_binding <<'\n';

   #ifdef time
       struct timeval end;
       gettimeofday(&end, NULL);
       timeAddColumn += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
   #endif

   }

table_t* Joiner::CreateTableTFromId(unsigned rel_id, unsigned rel_binding) {

#ifdef time
    struct timeval start;
    gettimeofday(&start, NULL);
#endif

    /* Get realtion */
    Relation & rel = getRelation(rel_id);

    /* Crate - Initialize a table_t */
    table_t *const table_t_ptr = new table_t;
    table_t_ptr->column_j = new column_t;
    table_t_ptr->intermediate_res = false;
    table_t_ptr->tups_num = rel.size;
    table_t_ptr->rels_num = 1;
    table_t_ptr->row_ids = NULL;

    /* Keep a mapping with the rowids table and the relaito ids na bindings */
    table_t_ptr->relations_bindings.insert(make_pair(rel_binding, 0));

#ifdef time
    struct timeval end;
    gettimeofday(&end, NULL);
    timeCreateTable += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
#endif

    return table_t_ptr;
}

table_t* Joiner::join(table_t *table_r, table_t *table_s, PredicateInfo &pred_info, columnInfoMap & cmap, bool isRoot, std::vector<SelectInfo> selections, int leafs, string & result_str) {

    #ifdef time
    struct timeval start;
    gettimeofday(&start, NULL);
    #endif

    relation_t * r1;
    relation_t * r2;

    #ifdef time
    struct timeval end;
    gettimeofday(&end, NULL);
    timeCreateRelationT += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
    gettimeofday(&start, NULL);
    #endif

    //HERE WE CHECK FOR CACHED PARTITIONS
    Cacheinf c;

    Selection left(pred_info.left);
    Selection right(pred_info.right);

    /* Debug orest */

    if (leafs&1) {
        if (idxcache.find(left) != idxcache.end()) {
            r1 = (relation_t *)malloc(sizeof(relation_t));
            r1->num_tuples = table_r->tups_num;
            c.R = idxcache[left];
        }
        else {

            r1 = CreateRelationT(table_r, pred_info.left);
            c.R = (cached_t *) calloc(THREAD_NUM, sizeof(cached_t));
        }
    } else {

        r1 = CreateRelationT(table_r, pred_info.left);
        c.R = NULL;
    }

    if (leafs&2) {
        if (idxcache.find(right) != idxcache.end()) {
            r2 = (relation_t *)malloc(sizeof(relation_t));
            r2->num_tuples = table_s->tups_num;
            c.S = idxcache[right];
        }
        else {
            r2 = CreateRelationT(table_s, pred_info.right);
            c.S = (cached_t *) calloc(THREAD_NUM, sizeof(cached_t));;
        }
    } else {
        r2 = CreateRelationT(table_s, pred_info.right);
        c.S = NULL;
    }


    result_t * res  = PRO(r1, r2, THREAD_NUM, c);

    if (leafs&1) {
        free(r1);
        idxcache[left] = c.R;
    }
    if (leafs&2) {
        free(r2);
        idxcache[right] = c.S;
    }


    #ifdef time
    gettimeofday(&end, NULL);
    timeRadixJoin += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
    gettimeofday(&start, NULL);
    #endif

    table_t *temp = NULL;
    // On root dont create a resilting table just get the checksums
    if (isRoot) {
        CheckSumOnTheFly(res, table_r, table_s, cmap, selections, result_str);
    } else {
        temp = CreateTableT(res, table_r, table_s, cmap);
    }


    #ifdef time
    gettimeofday(&end, NULL);
    timeCreateTableT += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
    #endif


    /* Free the tables and the result of radix */
    free(table_r->row_ids);
    delete table_r->column_j;
    delete table_r;
    free(table_s->row_ids);
    delete table_s->column_j;
    delete table_s;
    free(res->resultlist);
    free(res);

    return temp;
}

//CHECK SUM FUNCTION
std::string Joiner::check_sum(SelectInfo &sel_info, table_t *table) {
    AddColumnToTableT(sel_info, table);

    #ifdef prints
    std::cerr << "IN checksum" << '\n';
    flush(cerr);
    #endif

    uint64_t* col = table->column_j->values;
    int  tbi = table->column_j->table_index;
    unsigned * row_ids = table->row_ids;
    unsigned   rels_num = table->rels_num;
    unsigned   size = table->tups_num;
    uint64_t sum = 0;

    if (size == 0) {
        return "NULL";
    }
    // else if (size < GRAINSIZE) {
    //     for (uint64_t i = 0 ; i < size; i++) {
    //         sum += col[row_ids[i*rels_num + tbi]];
    //     }
    //
    //     return to_string(sum);
    // }
    else {
        /* Create the Sum obj */
        CheckSumT cs( col, row_ids, rels_num, tbi );
        parallel_reduce( blocked_range<size_t>(0, size, GRAINSIZE), cs );

        return to_string( cs.my_sum );
    }
}

// Loads a relation from disk
void Joiner::addRelation(const char* fileName) {
    relations.emplace_back(fileName);
}

// Loads a relation from disk
Relation& Joiner::getRelation(unsigned relationId) {
    if (relationId >= relations.size()) {
        cerr << "Relation with id: " << relationId << " does not exist" << endl;
        throw;
    }
    return relations[relationId];
}

// Get the total number of relations
int Joiner::getRelationsCount() {
    return relations.size();
}

/* +---------------------+
   |The Column functions |
   +---------------------+ */

void PrintColumn(column_t *column) {
    /* Print the column's table id */
    std::cerr << "Column of table " << column->id << " and size " << column->size << '\n';

    /* Iterate over the column's values and print them */
    for (int i = 0; i < column->size; i++) {
        std::cerr << column->values[i] << '\n';
    }
}

int main(int argc, char* argv[]) {
    Joiner joiner;

    // Read join relations
    string line;
    while (getline(cin, line)) {
        if (line == "Done") break;
        joiner.addRelation(line.c_str());
    }

    #ifdef time
    struct timeval start;
    gettimeofday(&start, NULL);
    #endif

    // Create threads
    task_scheduler_init init(THREAD_NUM);

    // Create scheduler NUMA REG 0
    joiner.job_scheduler1.Init(THREAD_NUM_1CPU, 0);

    // Create scheduler NUMA REG 1
    //joiner.job_scheduler2.Init(THREAD_NUM_2CPU, 1);

    // Preparation phase (not timed)
    QueryPlan queryPlan;

    // Get the needed info of every column
    queryPlan.fillColumnInfo(joiner);


    #ifdef time
    struct timeval end;
    gettimeofday(&end, NULL);
    timePreparation += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
    #endif

    // The test harness will send the first query after 1 second.
    QueryInfo i;
    int q_counter = 0;
    while (getline(cin, line)) {
        if (line == "F") continue; // End of a batch

        // Parse the query
        #ifdef prints
        std::cerr << q_counter  << ":" << line << '\n';
        #endif
        i.parseQuery(line);
        cleanQuery(i);
        q_counter++;

        #ifdef time
        gettimeofday(&start, NULL);
        #endif

        JoinTree* optimalJoinTree;
        optimalJoinTree = queryPlan.joinTreePtr->build(i, queryPlan.columnInfos);

        #ifdef time
        gettimeofday(&end, NULL);
        timeTreegen += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
        gettimeofday(&start, NULL);
        #endif

        string result_str;
        table_t * result = optimalJoinTree->root->execute(optimalJoinTree->root, joiner, i, result_str);


        #ifdef time
        gettimeofday(&end, NULL);
        timeExecute += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
        #endif

        if (result_str.empty()) {
            #ifdef time
            gettimeofday(&start, NULL);
            #endif

            //Compute the selection predicates
            string result_str;
            uint64_t checksum = 0;
            unordered_map<string, string> cached_sums;
            vector<SelectInfo> &selections = i.selections;
            for (size_t i = 0; i < selections.size(); i++) {

                // Check if checksum is cached
                string key = to_string(selections[i].binding) + to_string(selections[i].colId);
                unordered_map<string, string>::const_iterator got = cached_sums.find(key);
                if (got != cached_sums.end()) {
                    result_str += got->second;
                } else {
                    //string str = joiner.check_sum(selections[i], result, pool, futures);
                    string str = joiner.check_sum(selections[i], result);
                    cached_sums.insert(make_pair(key, str));
                    result_str += str;
                }

                // Create the write check sum
                if (i != selections.size() - 1) {
                    result_str +=  " ";
                }
            }

            free(result->row_ids);
            delete result;

            #ifdef time
            gettimeofday(&end, NULL);
            timeCheckSum += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
            #endif

            // Print the result
            std::cout << result_str << endl;
            //std::cerr << result_str << endl;
        } else {
            std::cout << result_str << endl;
        }
    }

#ifdef time
    std::cerr << "timePreparation:     " << (long)(timePreparation * 1000) << endl;
    std::cerr << "timeTreegen:         " << (long)(timeTreegen * 1000) << endl;
    std::cerr << "timeSelectFilter:    " << (long)(timeSelectFilter * 1000) << endl;
    std::cerr << "(a)timeNonIntFilters:" << (long)(timeNonIntermediateFilters * 1000) << endl;
    std::cerr << "(b)timeIntFilters:   " << (long)(timeIntermediateFilters * 1000) << endl;
    // std::cerr << "--timeGreaterFilter: " << (long)(timeGreaterFilter * 1000) << endl;
    // std::cerr << "--timeLessFilter:    " << (long)(timeLessFilter * 1000) << endl;
    // std::cerr << "--timeEqualFilter:   " << (long)(timeEqualFilter * 1000) << endl;
    std::cerr << "timeSelfJoin:        " << (long)(timeSelfJoin * 1000) << endl;
    std::cerr << "timeAddColumn:       " << (long)(timeAddColumn * 1000) << endl;
    std::cerr << "timeCreateRelationT: " << (long)(timeCreateRelationT * 1000) << endl;
    std::cerr << "--timeCreateRelI:    " << (long)(timeCreateRelI * 1000) << endl;
    std::cerr << "--timeCreateRelNonI: " << (long)(timeCreateRelNonI * 1000) << endl;
    std::cerr << "timeCreateTableT:    " << (long)(timeCreateTableT * 1000) << endl;
    std::cerr << "--timeCSsOnTheFly:   " << (long)(timeCheckSumsOnTheFly * 1000) << endl;
    std::cerr << "--timeCTPrepear:     " << (long)(timeCTPrepear * 1000) << endl;
    std::cerr << "--timeCT1bucket:     " << (long)(timeCT1bucket * 1000) << endl;
    std::cerr << "--timeCTMoreBuckets: " << (long)(timeCTMoreBuckets * 1000) << endl;
    std::cerr << "timeRadixJoin:       " << (long)(timeRadixJoin * 1000) << endl;
    std::cerr << "timeCheckSum:        " << (long)(timeCheckSum * 1000) << endl;
    std::cerr << "timeCleanQuery:      " << (long)(timeCleanQuery * 1000) << endl;
    std::cerr << "timeExecute:         " << (long)(timeExecute * 1000) << endl;
    flush(std::cerr);
#endif

    return 0;
}
