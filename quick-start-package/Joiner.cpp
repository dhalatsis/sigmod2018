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
//#define prints

using namespace tbb;
using namespace std;

//#define TIME_DETAILS
//#include <sstream>
//string timeDetStr = "";

bool done_testing = false;

/* Timing variables */
extern double timeSelfJoin;
extern double timeSelectFilter;
double timeConstruct = 0;
double timeLowJoin = 0;
double timeCreateTable = 0;
double timeAddColumn = 0;
double timeTreegen = 0;
double timeCheckSum = 0;
double timeBuildPhase = 0;
double timeProbePhase = 0;
double timeRadixJoin = 0;
double timeCreateRelationT = 0;
double timeCreateTableT = 0;
double timeExecute = 0;
double timePreparation = 0;

int cleanQuery(QueryInfo &info) {
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

    return 0;
}

/* ================================ */
/* Table_t <=> Relation_t fuctnions */
/* ================================ */
relation_t * Joiner::CreateRelationT(table_t * table, SelectInfo &sel_info) {

#ifdef time
    struct timeval start;
    gettimeofday(&start, NULL);
#endif

    /* Create a new column_t for table */
    unsigned * row_ids = table->row_ids;

    /* Get the relation from joiner */
    Relation &rel = getRelation(sel_info.relId);
    uint64_t * values = rel.columns.at(sel_info.colId);

    /* Create the relation_t */
    relation_t * new_relation = gen_rel(table->tups_num);

    if (table->intermediate_res) {

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
        //tuple_t * tuples = new_relation->tuples;

        // /* Initialize the tuple array */
        // for (uint32_t i = 0; i < size; i++) {
        //     tuples[i].key     = values[row_ids[i*rel_num + table_index]];
        //     tuples[i].payload = i;
        // }

        RelationIntermediateCT rct( new_relation->tuples, values, row_ids, rel_num, table_index );
        parallel_for(blocked_range<size_t>(0,size, GRAINSIZE), rct);
    }
    else {
        /* Initialize relation */
        uint32_t size = table->tups_num;

        // tuple_t * tuples = new_relation->tuples;
        //
        // /* Initialize the tuple array */
        // for (uint32_t i = 0; i < size; i++) {
        //     tuples[i].key     = values[i];
        //     tuples[i].payload = i;
        // }

        RelationNonIntermediateCT rct( new_relation->tuples, values );
        parallel_for(blocked_range<size_t>(0,size, GRAINSIZE), rct);

    }

#ifdef time
    struct timeval end;
    gettimeofday(&end, NULL);
    timeCreateRelationT += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
#endif

    return new_relation;
}

int compare(const void * a, const void * b)
{
    return ( ((tuple_t*)a)->key - ((tuple_t*)b)->key );
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

    uint32_t idx = 0;  // POints to the right index on the res
    uint32_t tup_i;

    for (int th = 0; th < THREAD_NUM; th++) {
        chainedtuplebuffer_t * cb = (chainedtuplebuffer_t *) result->resultlist[th].results;

        /* Get the touples form the results */
        tuplebuffer_t * tb = cb->buf;
        uint32_t numbufs = cb->numbufs;
        uint32_t row_i;

        /* Depending on tables choose what to pass */
        if (table_r->intermediate_res && table_s->intermediate_res) {
            TableAllIntermediateCT tct
            (
                tb->tuples,
                rids_res, rids_r, rids_s,
                &help_v_r, &help_v_s,
                idx, old_relnum_r, old_relnum_s, num_relations
            );
            parallel_for(blocked_range<size_t>(0,cb->writepos, GRAINSIZE), tct);
        }
        else if (table_r->intermediate_res) {
            TableRIntermediateCT tct
            (
                tb->tuples,
                rids_res, rids_r, rids_s,
                &help_v_r, &help_v_s,
                idx, old_relnum_r, old_relnum_s, num_relations
            );
            parallel_for(blocked_range<size_t>(0,cb->writepos, GRAINSIZE), tct);
        }
        else if (table_s->intermediate_res) {
            TableSIntermediateCT tct
            (
                tb->tuples,
                rids_res, rids_r, rids_s,
                &help_v_r, &help_v_s,
                idx, old_relnum_r, old_relnum_s, num_relations
            );
            parallel_for(blocked_range<size_t>(0,cb->writepos, GRAINSIZE), tct);
        }
        else {
            TableNoneIntermediateCT tct
            (
                tb->tuples,
                rids_res, rids_r, rids_s,
                &help_v_r, &help_v_s,
                idx, old_relnum_r, old_relnum_s, num_relations
            );
            parallel_for(blocked_range<size_t>(0,cb->writepos, GRAINSIZE), tct);
        }


        /* --------------------------------------------------------------------------------------
        The N-1 buffer loops , where the num of tups are CHAINEDBUFF_NUMTUPLESPERBUF
        ---------------------------------------------------------------------------------------- */
        tb = tb->next;
        idx += cb->writepos;
        for (uint32_t buf_i = 0; buf_i < numbufs - 1; buf_i++) {

            if (table_r->intermediate_res && table_s->intermediate_res){
                TableAllIntermediateCT tct
                (
                    tb->tuples,
                    rids_res, rids_r, rids_s,
                    &help_v_r, &help_v_s,
                    idx, old_relnum_r, old_relnum_s, num_relations
                );
                parallel_for(blocked_range<size_t>(0,CHAINEDBUFF_NUMTUPLESPERBUF, GRAINSIZE), tct);

            }
            else if (table_r->intermediate_res) {
                TableRIntermediateCT tct
                (
                    tb->tuples,
                    rids_res, rids_r, rids_s,
                    &help_v_r, &help_v_s,
                    idx, old_relnum_r, old_relnum_s, num_relations
                );
                parallel_for(blocked_range<size_t>(0,CHAINEDBUFF_NUMTUPLESPERBUF, GRAINSIZE), tct);
            }
            else if (table_s->intermediate_res) {
                TableSIntermediateCT tct
                (
                    tb->tuples,
                    rids_res, rids_r, rids_s,
                    &help_v_r, &help_v_s,
                    idx, old_relnum_r, old_relnum_s, num_relations
                );
                parallel_for(blocked_range<size_t>(0,CHAINEDBUFF_NUMTUPLESPERBUF, GRAINSIZE), tct);
            }
            else {
                TableNoneIntermediateCT tct
                (
                    tb->tuples,
                    rids_res, rids_r, rids_s,
                    &help_v_r, &help_v_s,
                    idx, old_relnum_r, old_relnum_s, num_relations
                );
                parallel_for(blocked_range<size_t>(0,CHAINEDBUFF_NUMTUPLESPERBUF, GRAINSIZE), tct);
            }

            /* Go the the next buffer */
            idx += CHAINEDBUFF_NUMTUPLESPERBUF;
            tb = tb->next;
        }

        /* Free cb */
        chainedtuplebuffer_free(cb);
    }

#ifdef time
    struct timeval end;
    gettimeofday(&end, NULL);
    timeCreateTableT += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
#endif

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

table_t* Joiner::join(table_t *table_r, table_t *table_s, PredicateInfo &pred_info, columnInfoMap & cmap) {

    #ifdef prints
    std::cerr << "Before creating Rels" << '\n';
    flush(cerr);
    #endif

    relation_t * r1 = CreateRelationT(table_r, pred_info.left);
    relation_t * r2 = CreateRelationT(table_s, pred_info.right);

    #ifdef prints
    std::cerr << "Created Rels" << '\n';
    flush(cerr);
    #endif

#ifdef TIME_DETAILS
    struct timeval start, end;
    gettimeofday(&start, NULL);
#endif

#ifdef time
    struct timeval start;
    gettimeofday(&start, NULL);
#endif

    result_t * res  = PRO(r1, r2, THREAD_NUM);

    #ifdef prints
    std::cerr << "After RJ" << '\n';
    flush(cerr);
    #endif

#ifdef time
    struct timeval end;
    gettimeofday(&end, NULL);
    timeRadixJoin += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
#endif

#ifdef TIME_DETAILS
    gettimeofday(&end, NULL);
    double dt = (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
    // std::ostringstream strs;
    if(!done_testing) {
        cerr << "RJ: " << dt << " sec" << endl;
        flush(cerr);
        // timeDetStr.append(strs.str());
    }
#endif

#ifdef TIME_DETAILS
    gettimeofday(&start, NULL);
#endif
    table_t *temp = CreateTableT(res, table_r, table_s, cmap);
#ifdef TIME_DETAILS
    gettimeofday(&end, NULL);
    dt = (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
    if(!done_testing) {
        cerr << "CreateTableT: " << dt << "sec for " << res->totalresults << " results" << endl;
        flush(cerr);
    }
#endif

    /* Free the tables */
    free(table_r->row_ids);
    delete table_r;
    free(table_s->row_ids);
    delete table_s;

    return temp;
}

#ifdef HOT_STUFF

// utility function for for_join
vector<table_t*> Joiner::getTablesFromTree(JTree* jTreePtr) {
    // cerr << "getTablesFromTree in" << endl;

    JTree *left = jTreePtr->left, *right = jTreePtr->right;
    table_t *table_l, *table_r, *res;
    vector<table_t*> tableVec;

    if (left == NULL && right == NULL) {
        // cerr << "a1" << endl;
        res = this->CreateTableTFromId(jTreePtr->node_id, jTreePtr->binding_id);
        tableVec.push_back(res);
        // cerr << "a2" << endl;
        return tableVec;
    }

    tableVec = this->getTablesFromTree(left);
    table_l = tableVec[0];

    /* Its join for sure */
    if (right != NULL) {
        // cerr << "b1" << endl;
        vector<table_t*> temp = this->getTablesFromTree(right);
        tableVec.insert(tableVec.end(), temp.begin(), temp.end());
        table_r = tableVec[1];
        // cerr << "b2" << endl;
        this->AddColumnToTableT(jTreePtr->predPtr->left, table_l);
        this->AddColumnToTableT(jTreePtr->predPtr->right, table_r);
        /* Filter on right ? */
        // this->AddColumnToTableT(jTreePtr->predPtr->left, table_l);
        // this->AddColumnToTableT(jTreePtr->predPtr->right, table_r);
        // res = joiner.join(table_l, table_r, *jTreePtr->predPtr);
        return tableVec;
    }
    /* Fiter or predicate to the same table (simple constraint)*/
    else {
        if (jTreePtr->filterPtr == NULL) {
            // cerr << "c1" << endl << "c2" << endl;
            // res = joiner.SelfJoin(table_l, jTreePtr->predPtr);
            tableVec.push_back(table_l);
            return tableVec;
        }
        // table filter
        else {
            // cerr << "d1" << endl;
            FilterInfo &filter = *(jTreePtr->filterPtr);
            this->AddColumnToTableT(jTreePtr->filterPtr->filterColumn, table_l);
            // cerr << "d1.5" << endl;
            this->Select(filter, table_l);
            // cerr << "d2" << endl;
            // tableVec.push_back(table_l);
            return tableVec;
        }
    }
}

/*  join relations using for_2, for_3, for_4 --
    traverse the JTree in a DFS manner and form of a vector of it's tables (filtered if required)
    and a map of vectors with the selected columns of each table,
    then call the appropriate for_n n=2,3,4 implementation */
void Joiner::for_join(JTree* jTreePtr, vector<SelectInfo> selections) {
    // cerr << "for_join in" << endl;
    // tables vector
    vector<table_t*> tableVec = this->getTablesFromTree(jTreePtr);
    assert(tableVec.size() => 2 && tableVec.size() <= 4);

    // selected columns map
    vector< vector<SelectInfo> > columns(1);
    for (auto s : selections) {
        // cerr << s.binding << "->" << s.colId << endl;
        if (columns.size() <= s.binding)
            columns.resize(columns.size()+1);
        columns[s.binding].push_back(s);
    }

    // join tables and print the required check-sum "on-the-fly"
    if (tableVec.size() == 2)
        this->for_2(tableVec[0], tableVec[1], columns);
    else if (tableVec.size() == 3)
        this->for_3(tableVec[0], tableVec[1], tableVec[2], columns);
    else    // if (tableVec.size() == 4)
        this->for_4(tableVec[0], tableVec[1], tableVec[2], tableVec[3], columns);

    // cerr << "for_join out" << endl;
}

// for_2 join UNOPTIMIZED
// columns maps the SELECTed columns of each table to the respective table
// we assume that table_a->column_j->size <= table_b->column_j->size
void Joiner::for_2(table_t* table_a, table_t* table_b, vector< vector<SelectInfo> > columns) {
    // cerr << "for_2 in" << endl;
    string result_str;
    // we will need the values for the check_sum
    // colVec maps the values of the SELECTed columns of each table to the respective table
    vector< vector<uint64_t*> > colVec(1);
    // cerr << "a1" << endl;
    assert(columns.size() == 2);
    for (uint64_t i = 0; i < columns.size(); i++) {
        if (i == colVec.size())
            colVec.resize(colVec.size()+1);
        for(uint64_t j = 0; j < columns[i].size(); j++) {
            vector<uint64_t*> temp = this->getRelation(columns[i][j].relId).columns;
            colVec[i].push_back(temp[columns[i][j].colId]);
        }
    }
    // cerr << "a2" << endl;
    // check_sum_vec maps the check_sum of each SELECTed column of each table to the respective table
    vector< vector<uint64_t> > check_sum_vec(1);
    for (uint64_t i = 0; i < columns.size(); i++) {
        if (i == check_sum_vec.size())
            check_sum_vec.resize(check_sum_vec.size()+1);
        for (uint64_t j = 0; j < columns[i].size(); j++)
            check_sum_vec[i].push_back(0);
    }
    // cerr << "a2" << endl;
    /* create hash_table for the hash_join phase */
    std::unordered_multimap<uint64_t, hash_entry> hash_c;
    /* hash_size->size of the hashtable,iter_size->size to iterate over to find same vals */
    uint64_t hash_size, iter_size;
    column_t *hash_col, *iter_col;

    /* check on wich table will create the hash_table */
    hash_size = table_a->column_j->size;
    hash_col = table_a->column_j;
    matrix &h_rows = *table_a->relations_row_ids;

    iter_size = table_b->column_j->size;
    iter_col = table_b->column_j;
    matrix &i_rows = *table_b->relations_row_ids;

    // cerr << "b1" << endl;
    /* now put the values of the column_r in the hash_table(construction phase) */
    for (uint64_t i = 0; i < hash_size; i++) {
        /* store hash[value of the column] = {rowid, index} */
        hash_entry hs;
        hs.row_id = h_rows[hash_col->table_index][i];
        hs.index = i;
        hash_c.insert({hash_col->values[i], hs});
    }
    // cerr << "b2" << endl;
    // cerr << "c1" << endl;
    /* now the phase of hashing */
    for (uint64_t i = 0; i < iter_size; i++) {
        /* remember we may have multi vals in 1 key,if it isnt a primary key */
        // cerr << columns.size() << " vs " << i << endl;
        /* vals->first = key ,vals->second = value */
        auto range_vals = hash_c.equal_range(iter_col->values[i]);
        for (auto &vals = range_vals.first; vals != range_vals.second; vals++) {
            for (uint64_t j = 0; j < check_sum_vec.size(); j++) {
                // cerr << "c1.5 a" << endl;
                for(uint64_t k = 0; k < check_sum_vec[j].size(); k++) {
                    // cerr << "c1.75 " << j << " " << k << " " << i << " vs " << colVec.size() << " " << colVec[j].size() << endl;
                    // cerr << "c1.75 " << j << " " << k << " " << i << "| " << h_rows[j][vals->second.index] << " " << i_rows[j][i] << " vs " << colVec.size() << " " << colVec[j].size() << endl;
                    // check_sum_vec[j][k] += colVec[j][k][i];
                    for (uint64_t l = 0 ; l < h_rows.size(); l++)
                        check_sum_vec[j][k] += h_rows[l][vals->second.index];
                    // cerr << "c1.75 b" << endl;
                    /* then go to the s's row ids to get the values */
                    for (uint64_t l = 0; l < i_rows.size(); l++)
                        check_sum_vec[j][k] += i_rows[l][i];
                }
                // cerr << "c1.5 b" << endl;
            }
        }
    }
    // cerr << "c2" << endl;
    /* do the cleaning */
    delete table_a->relations_row_ids;
    delete table_b->relations_row_ids;

    // cerr << "d1" << endl;
    // print checksums
    for (uint64_t i = 0; i < check_sum_vec.size(); i++) {
        for (uint64_t j = 0; j < check_sum_vec[i].size(); j++) {
            // cerr << check_sum_vec[i][j] << endl;
            result_str += to_string(check_sum_vec[i][j]);
            if (j < check_sum_vec[i].size()-1 || i < check_sum_vec.size()-1)
                result_str += " ";
        }
    }
    cout << result_str << endl;
    // cerr << "d2" << endl;
    // cerr << "for_2 out" << endl;
}


// for_3 join UNOPTIMIZED
// columns maps the SELECTed columns of each table to the respective table
// we assume that table_a->column_j->size <= table_b->column_j->size <= table_c->column_j->size

void Joiner::for_3(table_t* table_a, table_t* table_b, table_t* table_c, vector< vector<SelectInfo> > columns) {
    cerr << "for_3 in" << endl;
    // string result_str;
    // // we will need the values for the check_sum
    // // colVec maps the values of the SELECTed columns of each table to the respective table
    // unordered_map< uint64_t, vector<relation_t*> > colVec;
    // for (uint64_t i = 0; i < columns.size(); i++) {
    //     for(auto c : columns[i]) {
    //         SelectInfo temp;
    //         temp.relId = c.relId;
    //         temp.binding = c.binding;
    //         temp.colId = c.colId;
    //         if (i == 1) colVec[i].push_back(CreateRelationT(table_a, temp));
    //         else colVec[i].push_back(CreateRelationT(table_b, temp));
    //     }
    // }
    // // check_sum_vec maps the check_sum of each SELECTed column of each table to the respective table
    // unordered_map< uint64_t, vector<uint64_t> > check_sum_vec;
    // for (uint64_t i = 0; i <= 2; i++)
    //     for (uint64_t j = 0; j < columns[i].size(); j++)
    //         check_sum_vec[i].push_back(0);
    // /* create hash_table for the hash_join phase */
    // std::unordered_multimap<uint64_t, hash_entry> hash_c, hash_c2;
    // /* hash_size->size of the hashtable,iter_size->size to iterate over to find same vals */
    // uint64_t hash_size, iter_size, iter_size2;
    // column_t *hash_col, *iter_col, *iter_col2;
    //
    // /* check on wich table will create the hash_table */
    // hash_size = table_a->column_j->size;
    // hash_col = table_a->column_j;
    // matrix &h_rows = *table_a->relations_row_ids;
    //
    // iter_size = table_b->column_j->size;
    // iter_col = table_b->column_j;
    // matrix &i_rows = *table_b->relations_row_ids;
    //
    // iter_size2 = table_c->column_j->size;
    // iter_col2 = table_c->column_j;
    // matrix &i_rows2 = *table_c->relations_row_ids;
    //
    // /* now put the values of the column_r in the hash_table(construction phase) */
    // for (uint64_t i = 0; i < hash_size; i++) {
    //     /* store hash[value of the column] = {rowid, index} */
    //     hash_entry hs;
    //     hs.row_id = h_rows[hash_col->table_index][i];
    //     hs.index = i;
    //     hash_c.insert({hash_col->values[i], hs});
    // }
    // /* now the first phase of hashing */
    // for (uint64_t i = 0; i < iter_size; i++) {
    //     /* remember we may have multi vals in 1 key,if it isnt a primary key */
    //     /* vals->first = key ,vals->second = value */
    //     auto range_vals = hash_c.equal_range(iter_col->values[i]);
    //     for(auto &vals = range_vals.first; vals != range_vals.second; vals++) {
    //         hash_entry hs;
    //         hs.row_id = i_rows[iter_col->table_index][i];
    //         hs.index = i;
    //         hash_c2.insert({iter_col->values[i], hs});
    //     }
    // }
    // /* now the second phase of hashing */
    // for (uint64_t i = 0; i < iter_size2; i++) {
    //     /* remember we may have multi vals in 1 key,if it isnt a primary key */
    //     /* vals->first = key ,vals->second = value */
    //     auto range_vals = hash_c2.equal_range(iter_col2->values[i]);
    //     for(auto &vals = range_vals.first; vals != range_vals.second; vals++) {
    //         for (uint64_t j = 0; j <= 2; j++) {
    //             for(uint64_t k = 0; k < columns[j].size(); k++) {
    //                 check_sum_vec[j][k] += colVec[j][k]->tuples[i].payload;
    //             }
    //         }
    //     }
    // }
    // /* do the cleaning */
    // delete table_a->relations_row_ids;
    // delete table_b->relations_row_ids;
    // delete table_c->relations_row_ids;
    //
    // // print checksums
    // for (uint64_t i = 0; i < check_sum_vec.size(); i++) {
    //     for (uint64_t j = 0; j < check_sum_vec[i].size(); j++) {
    //         // cerr << check_sum_vec[i][j] << endl;
    //         result_str += to_string(check_sum_vec[i][j]);
    //         if (j < check_sum_vec[i].size()-1 || i < check_sum_vec.size()-1)
    //             result_str += " ";
    //     }
    // }
    // cout << result_str << endl;

    cerr << "for_3 out" << endl;
}

// for_4 join UNOPTIMIZED
// columns maps the SELECTed columns of each table to the respective table
// we assume that table_a->column_j->size <= table_b->column_j->size <= table_c->column_j->size <= table_d->column_j->size

void Joiner::for_4(table_t* table_a, table_t* table_b, table_t* table_c, table_t* table_d, vector< vector<SelectInfo> > columns) {
    cerr << "for_4 in" << endl;
    // string result_str;
    // // we will need the values for the check_sum
    // // colVec maps the values of the SELECTed columns of each table to the respective table
    // unordered_map< uint64_t, vector<relation_t*> > colVec;
    // for (uint64_t i = 0; i < columns.size(); i++) {
    //     for(auto c : columns[i]) {
    //         SelectInfo temp;
    //         temp.relId = c.relId;
    //         temp.binding = c.binding;
    //         temp.colId = c.colId;
    //         if (i == 1) colVec[i].push_back(CreateRelationT(table_a, temp));
    //         else colVec[i].push_back(CreateRelationT(table_b, temp));
    //     }
    // }
    // // check_sum_vec maps the check_sum of each SELECTed column of each table to the respective table
    // unordered_map< uint64_t, vector<uint64_t> > check_sum_vec;
    // for (uint64_t i = 0; i <= 3; i++)
    //     for (uint64_t j = 0; j < columns[i].size(); j++)
    //         check_sum_vec[i].push_back(0);
    // /* create hash_table for the hash_join phase */
    // std::unordered_multimap<uint64_t, hash_entry> hash_c, hash_c2, hash_c3;
    // /* hash_size->size of the hashtable,iter_size->size to iterate over to find same vals */
    // uint64_t hash_size, iter_size, iter_size2, iter_size3;
    // column_t *hash_col, *iter_col, *iter_col2, *iter_col3;
    //
    // /* check on wich table will create the hash_table */
    // hash_size = table_a->column_j->size;
    // hash_col = table_a->column_j;
    // matrix &h_rows = *table_a->relations_row_ids;
    //
    // iter_size = table_b->column_j->size;
    // iter_col = table_b->column_j;
    // matrix &i_rows = *table_b->relations_row_ids;
    //
    // iter_size2 = table_c->column_j->size;
    // iter_col2 = table_c->column_j;
    // matrix &i_rows2 = *table_c->relations_row_ids;
    //
    // iter_size3 = table_d->column_j->size;
    // iter_col3 = table_d->column_j;
    // matrix &i_rows3 = *table_d->relations_row_ids;
    //
    // /* now put the values of the column_r in the hash_table(construction phase) */
    // for (uint64_t i = 0; i < hash_size; i++) {
    //     /* store hash[value of the column] = {rowid, index} */
    //     hash_entry hs;
    //     hs.row_id = h_rows[hash_col->table_index][i];
    //     hs.index = i;
    //     hash_c.insert({hash_col->values[i], hs});
    // }
    // /* now the first phase of hashing */
    // for (uint64_t i = 0; i < iter_size; i++) {
    //     /* remember we may have multi vals in 1 key,if it isnt a primary key */
    //     /* vals->first = key ,vals->second = value */
    //     auto range_vals = hash_c.equal_range(iter_col->values[i]);
    //     for(auto &vals = range_vals.first; vals != range_vals.second; vals++) {
    //         hash_entry hs;
    //         hs.row_id = i_rows[iter_col->table_index][i];
    //         hs.index = i;
    //         hash_c2.insert({iter_col->values[i], hs});
    //     }
    // }
    // /* now the second phase of hashing */
    // for (uint64_t i = 0; i < iter_size2; i++) {
    //     /* remember we may have multi vals in 1 key,if it isnt a primary key */
    //     /* vals->first = key ,vals->second = value */
    //     auto range_vals = hash_c2.equal_range(iter_col2->values[i]);
    //     for(auto &vals = range_vals.first; vals != range_vals.second; vals++) {
    //         hash_entry hs;
    //         hs.row_id = i_rows2[iter_col2->table_index][i];
    //         hs.index = i;
    //         hash_c3.insert({iter_col2->values[i], hs});
    //     }
    // }
    // /* now the third phase of hashing */
    // for (uint64_t i = 0; i < iter_size3; i++) {
    //     /* remember we may have multi vals in 1 key,if it isnt a primary key */
    //     /* vals->first = key ,vals->second = value */
    //     auto range_vals = hash_c3.equal_range(iter_col3->values[i]);
    //     for(auto &vals = range_vals.first; vals != range_vals.second; vals++) {
    //         for (uint64_t j = 0; j <= 3; j++) {
    //             for(uint64_t k = 0; k < columns[j].size(); k++) {
    //                 check_sum_vec[j][k] += colVec[j][k]->tuples[i].payload;
    //             }
    //         }
    //     }
    // }
    // /* do the cleaning */
    // delete table_a->relations_row_ids;
    // delete table_b->relations_row_ids;
    // delete table_c->relations_row_ids;
    // delete table_d->relations_row_ids;
    //
    // // print checksums
    // for (uint64_t i = 0; i < check_sum_vec.size(); i++) {
    //     for (uint64_t j = 0; j < check_sum_vec[i].size(); j++) {
    //         // cerr << check_sum_vec[i][j] << endl;
    //         result_str += to_string(check_sum_vec[i][j]);
    //         if (j < check_sum_vec[i].size()-1 || i < check_sum_vec.size()-1)
    //             result_str += " ";
    //     }
    // }
    // cout << result_str << endl;

    cerr << "for_4 out" << endl;
}
#endif

void Joiner::construct(table_t *table) {
// #ifdef time
//     struct timeval start;
//     gettimeofday(&start, NULL);
// #endif
//
//     /* Innitilize helping variables */
//     column_t &column = *table->column_j;
//     const uint64_t *column_values = column.values;
//     const int       table_index   = column.table_index;
//     const uint64_t  column_size   = table->relations_row_ids->operator[](table_index).size();
//     matrix &row_ids = *table->relations_row_ids;
//
//     /* Create a new value's array  */
//     uint64_t *const new_values  = new uint64_t[column_size];
//
//     /* construct a new array with the row ids that it's sorted */
//     //vector<int> rids = row_ids[table_index];  //cp construct
//     //std::sort(rids.begin(), rids.end());
//
//     /* Pass the values of the old column to the new one, based on the row ids of the joiner */
//     for (int i = 0; i < column_size; i++) {
//     	new_values[i] = column_values[row_ids[table_index][i]];//rids[i]];
//     }
//
//     /* Update the column of the table */
//     column.values = new_values;
//     column.size   = column_size;
//
// #ifdef time
//     struct timeval end;
//     gettimeofday(&end, NULL);
//     timeConstruct += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
// #endif
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
    else if (size < 1000) {
        for (uint64_t i = 0 ; i < size; i++) {
            sum += col[row_ids[i*rels_num + tbi]];
        }

        return to_string(sum);
    }
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

// Hashes a value and returns a check-sum
// The check should be NULL if there is no qualifying tuple
void Joiner::join(QueryInfo& i) {}

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

    // Preparation phase (not timed)
    QueryPlan queryPlan;

    // Get the needed info of every column
    queryPlan.fillColumnInfo(joiner);

#ifdef TIME_DETAILS
    done_testing = false;
    cerr << endl;
    // bool its_over = false;
    for (int i = 1000; i <= 41000; i += 10000) {
        for (int j = 1000; j <= 41000; j += 10000) {
            vector<table_t*> tables;
            tables.push_back(joiner.CreateTableTFromId(7, 7));
            tables.push_back(joiner.CreateTableTFromId(13, 13));
            if (i <= tables[0]->relations_row_ids[0][0].size()) {
                for (int k = 0; k < tables[0]->relations_row_ids[0].size(); k++)
                    tables[0]->relations_row_ids[k][0].resize(i);
            } else {
                tables[0] = NULL;
            }
            if (j <= tables[1]->relations_row_ids[0][0].size()) {
                for (int k = 0; k < tables[1]->relations_row_ids[0].size(); k++)
                    tables[1]->relations_row_ids[k][0].resize(j);
            } else {
                tables[1] = NULL;
            }

            if (!tables[0] || !tables[1]) {
                // its_over = true;
                break;
            }

            PredicateInfo predicate;
            // predicate.left.relId = 13;
            // predicate.left.binding = 0;
            // predicate.left.colId = 1;
            // predicate.right.relId = 13;
            // predicate.right.binding = 1;
            // predicate.right.colId = 1;
            struct timeval start, end;
            gettimeofday(&start, NULL);
            // joiner.join(tables[0], tables[0], predicate);
            gettimeofday(&end, NULL);
            double dt = (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
            // std::ostringstream strs;
            // cerr << "13,\t" << i << ",\t13,\t" << j << ",\t" << dt << " sec" << endl;
            // flush(cerr);
            // timeDetStr.append(strs.str());

            predicate.left.relId = 7;
            predicate.left.binding = 0;
            predicate.left.colId = 1;
            predicate.right.relId = 13;
            predicate.right.binding = 1;
            predicate.right.colId = 1;
            gettimeofday(&start, NULL);
            joiner.join(tables[0], tables[1], predicate);
            gettimeofday(&end, NULL);
            dt = (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
            cerr << "7,\t" << i << ",\t13,\t" << j << ",\t" << dt << "sec" << endl;
        }
        // if (its_over)
        //     break;
    }
    cerr << timeDetStr << endl;
    done_testing = true;
#endif

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
        // JTree *jTreePtr;
        // if (i.predicates.size() == 1)
        //     jTreePtr = treegen(&i);
        // else                            // Create the optimal join tree
            optimalJoinTree = queryPlan.joinTreePtr->build(i, queryPlan.columnInfos);
        //optimalJoinTree->root->print(optimalJoinTree->root);

        #ifdef time
        gettimeofday(&end, NULL);
        timeTreegen += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
        #endif

        #ifdef time
        gettimeofday(&start, NULL);
        #endif

        table_t *result;
        // if (i.predicates.size() == 1)
        //     joiner.for_join(jTreePtr, i.selections);
        // else
            result = optimalJoinTree->root->execute(optimalJoinTree->root, joiner, i);
        // table_t * result = jTreeMakePlan(jTreePtr, joiner);

        #ifdef time
        gettimeofday(&end, NULL);
        timeExecute += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
        #endif

        #ifdef time
        gettimeofday(&start, NULL);
        #endif

        // Compute the selection predicates
        string result_str;
        uint64_t checksum = 0;
        unordered_map<string, string> cached_sums;
        vector<SelectInfo> &selections = i.selections;
        // if (i.predicates.size() != 1) {
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

            #ifdef time
            gettimeofday(&end, NULL);
            timeCheckSum += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
            #endif

            // Print the result
            std::cout << result_str << endl;
        // }
    }

#ifdef time
    std::cerr << "timePreparation: " << (long)(timePreparation * 1000) << endl;
    std::cerr << "timeCreateTableT: " << (long)(timeCreateTableT * 1000) << endl;
    std::cerr << "timeCreateRelationT: " << (long)(timeCreateRelationT * 1000) << endl;
    std::cerr << "timeConstruct: " << (long)(timeConstruct * 1000) << endl;
    std::cerr << "timeSelectFilter: " << (long)(timeSelectFilter * 1000) << endl;
    std::cerr << "timeSelfJoin: " << (long)(timeSelfJoin * 1000) << endl;
    std::cerr << "timeRadixJoin: " << (long)(timeRadixJoin * 1000) << endl;
    //std::cerr << "->timeBuildPhase: " << (long)(timeBuildPhase * 1000) << endl;
    //std::cerr << "->timeProbePhase: " << (long)(timeProbePhase * 1000) << endl;
    std::cerr << "timeAddColumn: " << (long)(timeAddColumn * 1000) << endl;
    std::cerr << "timeCreateTable: " << (long)(timeCreateTable * 1000) << endl;
    std::cerr << "timeTreegen: " << (long)(timeTreegen * 1000) << endl;
    std::cerr << "timeCheckSum: " << (long)(timeCheckSum * 1000) << endl;
    std::cerr << "timeExecute: " << (long)(timeExecute * 1000) << endl;
    flush(std::cerr);
#endif

    return 0;
}
