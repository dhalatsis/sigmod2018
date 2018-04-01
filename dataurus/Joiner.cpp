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

using namespace std;

bool done_testing = false;

/* Timing variables */
extern double timeSelfJoin;
extern double timeSCSelfJoin;
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
    uint64_t s = rel.size;

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

        size_t range = THREAD_NUM_1CPU;  // always eqyal with threads of radix
        vector<uint64_t> sum(distinctPairs_in_R.size() + distinctPairs_in_S.size(), 0);
        vector<uint64_t*> sums(range);
        for (size_t i = 0; i < sums.size(); i++) {
            sums[i] = (uint64_t *) calloc (sum.size(), sizeof(uint64_t));
        }

        if (table_r->intermediate_res && table_s->intermediate_res) {

            struct interInterSum_arg a[range];
            for (int th = 0; th < range ; th++) {
                a[th].priv_checsums = sums[th];
                a[th].distinctPairs_r = &distinctPairs_in_R;
                a[th].distinctPairs_s = &distinctPairs_in_S;
                a[th].rel_num_r = table_r->rels_num;
                a[th].rel_num_s = table_s->rels_num;
                a[th].rids_r  = table_r->row_ids;
                a[th].rids_s  = table_s->row_ids;
                a[th].cb = (chainedtuplebuffer_t *) result->resultlist[th].results;
                job_scheduler1.Schedule(new JobCheckSumInterInter(a[th]));
            }
            job_scheduler1.Barrier();

            /* Create the checksum */
            for (size_t j = 0; j < sum.size(); j++) {
                for (size_t i = 0; i < sums.size(); i++) {
                    sum[j] += sums[i][j];
                }
            }
        }
        else if (table_r->intermediate_res) {

            struct interNoninterSum_arg a[range];
            for (int th = 0; th < range ; th++) {
                a[th].priv_checsums = sums[th];
                a[th].distinctPairs_r = &distinctPairs_in_R;
                a[th].distinctPairs_s = &distinctPairs_in_S;
                a[th].rel_num_r = table_r->rels_num;
                a[th].rids_r  = table_r->row_ids;
                a[th].cb = (chainedtuplebuffer_t *) result->resultlist[th].results;
                job_scheduler1.Schedule(new JobCheckSumInterNonInter(a[th]));
            }
            job_scheduler1.Barrier();

            /* Create the checksum */
            for (size_t j = 0; j < sum.size(); j++) {
                for (size_t i = 0; i < sums.size(); i++) {
                    sum[j] += sums[i][j];
                }
            }
        }
        else if (table_s->intermediate_res) {

            struct noninterInterSum_arg a[range];
            for (int th = 0; th < range ; th++) {
                a[th].priv_checsums = sums[th];
                a[th].distinctPairs_r = &distinctPairs_in_R;
                a[th].distinctPairs_s = &distinctPairs_in_S;
                a[th].rel_num_s = table_s->rels_num;
                a[th].rids_s  = table_s->row_ids;
                a[th].cb = (chainedtuplebuffer_t *) result->resultlist[th].results;
                job_scheduler1.Schedule(new JobCheckSumNonInterInter(a[th]));
            }
            job_scheduler1.Barrier();

            /* Create the checksum */
            for (size_t j = 0; j < sum.size(); j++) {
                for (size_t i = 0; i < sums.size(); i++) {
                    sum[j] += sums[i][j];
                }
            }
        }
        else {

            struct noninterNoninterSum_arg a[range];
            for (int th = 0; th < range ; th++) {
                a[th].priv_checsums = sums[th];
                a[th].distinctPairs_r = &distinctPairs_in_R;
                a[th].distinctPairs_s = &distinctPairs_in_S;
                a[th].cb = (chainedtuplebuffer_t *) result->resultlist[th].results;
                job_scheduler1.Schedule(new JobCheckSumNonInterNonInter(a[th]));
            }
            job_scheduler1.Barrier();

            /* Create the checksum */
            for (size_t j = 0; j < sum.size(); j++) {
                for (size_t i = 0; i < sums.size(); i++) {
                    sum[j] += sums[i][j];
                }
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

    /* Only one relation can be victimized */
    unsigned rel_num_r = table_r->relations_bindings.size();
    unsigned rel_num_s = table_s->relations_bindings.size();
    unordered_map<unsigned, unsigned>::iterator itr;
    bool victimize = true;
    int  index     = -1;
    char where     =  0;  // 1 is table R and 2 is table S
    for (itr = table_r->relations_bindings.begin(); itr != table_r->relations_bindings.end(); itr++) {
        victimize = true;
        for (columnInfoMap::iterator it=cmap.begin(); it != cmap.end(); it++) {
            if (it->first.binding == itr->first) {
                victimize = false;
                break;
            }
        }
        if (victimize) {
            index = itr->second;
            where = 1;
        }
    }

    // If it was not found sarch the other relation
    if (index != -1)
        for (itr = table_s->relations_bindings.begin(); itr != table_s->relations_bindings.end(); itr++) {
            victimize = true;
            for (columnInfoMap::iterator it=cmap.begin(); it != cmap.end(); it++) {
                if (it->first.binding == itr->first) {
                    victimize = false;
                    break;
                }
            }
            if (victimize) {
                index = itr->second;
                where = 2;
            }
        }

    /* Create - Initialize the new table_t */
    unsigned num_relations = (index == -1) ? rel_num_r + rel_num_s : rel_num_r + rel_num_s - 1;
    table_t * new_table = new table_t;
    new_table->intermediate_res = true;
    new_table->column_j = new column_t;
    new_table->tups_num = result->totalresults;
    new_table->rels_num = num_relations;
    new_table->row_ids = (unsigned *) malloc(sizeof(unsigned) * num_relations * result->totalresults);

    /* Create the new maping vector */
    int rem = 0;
    for (itr = table_r->relations_bindings.begin(); itr != table_r->relations_bindings.end(); itr++) {
        if (where == 1 && index == itr->second){
            rem = 1;
            continue;
        }else if (where == 1 && index < itr->second)
            new_table->relations_bindings.insert(make_pair(itr->first, itr->second - 1));
        else
            new_table->relations_bindings.insert(make_pair(itr->first, itr->second));
    }

    for (itr = table_s->relations_bindings.begin(); itr != table_s->relations_bindings.end(); itr++) {
        if (where == 2 && index == itr->second)
            continue;
        else if (where == 2 && index < itr->second)
            new_table->relations_bindings.insert(make_pair(itr->first, itr->second + rel_num_r - 1));
        else
            new_table->relations_bindings.insert(make_pair(itr->first, itr->second + rel_num_r - rem));
    }

    /* Get the 3 row_ids matrixes in referances */
    unsigned * rids_res = new_table->row_ids;
    unsigned * rids_r   = table_r->row_ids;
    unsigned * rids_s   = table_s->row_ids;

#ifdef time
    struct timeval end;
    gettimeofday(&end, NULL);
    timeCTPrepear += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
#endif

    uint32_t idx = 0;  // POints to the right index on the res
    uint32_t tup_i;
    size_t range = THREAD_NUM_1CPU; // + THREAD_NUM_2CPU;

    /* Find jobs number */
    int jobs_num = 0;
    for (size_t th = 0; th < range; th++) {
        chainedtuplebuffer_t * cb = (chainedtuplebuffer_t *) result->resultlist[th].results;
        jobs_num += cb->numbufs;
    }

    if (table_r->intermediate_res && table_s->intermediate_res) {
        //struct interInterTable_arg a[jobs_num];
        unsigned prefix = 0, inner_prefix = 0, buffs_prefix = 0;
        tuplebuffer_t * tb;

        /* Loop all the buffers */
        for (int th = 0; th < range ; th++) {
            chainedtuplebuffer_t * cb = (chainedtuplebuffer_t *) result->resultlist[th].results;
            inner_prefix = 0;
            tb = cb->buf;
            for (int buff = 0; buff < cb->numbufs && result->resultlist[th].nresults != 0; buff++) {
                struct interInterTable_arg * a =new interInterTable_arg;
                a->start_index = inner_prefix + prefix;
                a->rel_num_all = num_relations;
                a->rel_num_r = rel_num_r;
                a->rel_num_s = rel_num_s;
                a->rids_res  = rids_res;
                a->rids_r = rids_r;
                a->rids_s = rids_s;
                a->index  = index;
                a->where  = where;
                a->size   = (buff == 0) ? cb->writepos : CHAINEDBUFF_NUMTUPLESPERBUF;
                a->tb = tb;
                job_scheduler1.Schedule(new JobCreateInterInterTable(*a));
                inner_prefix += (buff == 0) ? cb->writepos : CHAINEDBUFF_NUMTUPLESPERBUF;
                tb = tb->next;
            }
            buffs_prefix += cb->numbufs;
            prefix += result->resultlist[th].nresults;
        }
        job_scheduler1.Barrier();
    }
    else if (table_r->intermediate_res) {
        //struct interNoninterTable_arg a[jobs_num];
        unsigned prefix = 0, inner_prefix = 0, buffs_prefix = 0;
        tuplebuffer_t * tb;

        /* Loop all the buffers */
        for (int th = 0; th < range ; th++) {
            chainedtuplebuffer_t * cb = (chainedtuplebuffer_t *) result->resultlist[th].results;
            inner_prefix = 0;
            tb = cb->buf;
            for (int buff = 0; buff < cb->numbufs && result->resultlist[th].nresults != 0; buff++) {
                struct interNoninterTable_arg * a = new interNoninterTable_arg;
                a->start_index = inner_prefix + prefix;
                a->rel_num_all = num_relations;
                a->rel_num_r = rel_num_r;
                a->rids_res  = rids_res;
                a->rids_r = rids_r;
                a->index  = index;
                a->where  = where;
                a->size   = (buff == 0) ? cb->writepos : CHAINEDBUFF_NUMTUPLESPERBUF;
                a->tb = tb;
                job_scheduler1.Schedule(new JobCreateInterNonInterTable(*a));
                inner_prefix += (buff == 0) ? cb->writepos : CHAINEDBUFF_NUMTUPLESPERBUF;
                tb = tb->next;
            }
            buffs_prefix += cb->numbufs;
            prefix += result->resultlist[th].nresults;
        }
        job_scheduler1.Barrier();
    }
    else if (table_s->intermediate_res) {
        //struct noninterInterTable_arg a[jobs_num];
        unsigned prefix = 0, inner_prefix = 0, buffs_prefix = 0;
        tuplebuffer_t * tb;

        /* Loop all the buffers */
        for (int th = 0; th < range ; th++) {
            chainedtuplebuffer_t * cb = (chainedtuplebuffer_t *) result->resultlist[th].results;
            inner_prefix = 0;
            tb = cb->buf;
            for (int buff = 0; buff < cb->numbufs && result->resultlist[th].nresults != 0; buff++) {
                struct noninterInterTable_arg * a =new noninterInterTable_arg;
                a->start_index = inner_prefix + prefix;
                a->rel_num_all = num_relations;
                a->rel_num_s = rel_num_s;
                a->rids_res  = rids_res;
                a->rids_s = rids_s;
                a->index  = index;
                a->where  = where;
                a->size   = (buff == 0) ? cb->writepos : CHAINEDBUFF_NUMTUPLESPERBUF;
                a->tb = tb;
                job_scheduler1.Schedule(new JobCreateNonInterInterTable(*a));
                inner_prefix += (buff == 0) ? cb->writepos : CHAINEDBUFF_NUMTUPLESPERBUF;
                tb = tb->next;
            }
            buffs_prefix += cb->numbufs;
            prefix += result->resultlist[th].nresults;
        }
        job_scheduler1.Barrier();
    }
    else {
        //struct noninterNoninterTable_arg a[jobs_num];
        unsigned prefix = 0, inner_prefix = 0, buffs_prefix = 0;
        tuplebuffer_t * tb;

        /* Loop all the buffers */
        for (int th = 0; th < range ; th++) {
            chainedtuplebuffer_t * cb = (chainedtuplebuffer_t *) result->resultlist[th].results;
            inner_prefix = 0;
            tb = cb->buf;
            for (int buff = 0; buff < cb->numbufs && result->resultlist[th].nresults != 0; buff++) {
                struct noninterNoninterTable_arg * a =new noninterNoninterTable_arg;
                a->start_index = inner_prefix + prefix;
                a->rel_num_all = num_relations;
                a->rids_res  = rids_res;
                a->index  = index;
                a->where  = where;
                a->size   = (buff == 0) ? cb->writepos : CHAINEDBUFF_NUMTUPLESPERBUF;
                a->tb = tb;
                job_scheduler1.Schedule(new JobCreateNonInterNonInterTable(*a));
                inner_prefix += (buff == 0) ? cb->writepos : CHAINEDBUFF_NUMTUPLESPERBUF;
                tb = tb->next;
            }
            buffs_prefix += cb->numbufs;
            prefix += result->resultlist[th].nresults;
        }
        job_scheduler1.Barrier();
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
    size_t threads = THREAD_NUM_1CPU; // + THREAD_NUM_2CPU;

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
            c.R = (cached_t *) calloc(threads, sizeof(cached_t));
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
            c.S = (cached_t *) calloc(threads, sizeof(cached_t));;
        }
    } else {
        r2 = CreateRelationT(table_s, pred_info.right);
        c.S = NULL;
    }

    result_t * res  = PRO(r1, r2, threads, c);

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
        //std::cerr << q_counter  << ":" << line << '\n';
        i.parseQuery(line);
        cleanQuery(i);
        q_counter++;

        #ifdef time
        gettimeofday(&start, NULL);
        #endif

        JoinTree* optimalJoinTree;
        optimalJoinTree = queryPlan.joinTreePtr->build(i, queryPlan.columnInfos);
        //optimalJoinTree->root->print(optimalJoinTree->root);

        #ifdef time
        gettimeofday(&end, NULL);
        timeTreegen += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
        gettimeofday(&start, NULL);
        #endif

        string result_str;
        bool stop = false;
        table_t* result = optimalJoinTree->root->execute(optimalJoinTree->root, joiner, i, result_str, &stop);

        // Print the result
        std::cout << result_str << endl;

        #ifdef time
        gettimeofday(&end, NULL);
        timeExecute += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
        #endif
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
