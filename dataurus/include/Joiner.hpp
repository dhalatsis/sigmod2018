#pragma once
#include <unordered_map>
#include <sys/time.h>
#include <string.h>
#include <algorithm>
#include <array>
#include <cstdio>
#include <iostream>
#include <thread>
#include <vector>
#include <cstdint>
#include <string>
#include <map>

#include "Relation.hpp"
#include "Parser.hpp"
#include "table_t.hpp"
#include "parallel_radix_join.h"
#include "tuple_buffer.h"
#include "prj_params.h"
#include "create_job.h"
#include "checksum_job.h"

/* THread pool Includes */
/*----------------*/

#define time
//#define prints
#define THREAD_NUM_1CPU 4 //20
#define THREAD_NUM_2CPU 0

using namespace std;


class JTree;
struct ColumnInfo;
typedef std::map<SelectInfo, ColumnInfo> columnInfoMap;

/*
 * Prints a column
 * Arguments : A @column of column_t type
 */
void PrintColumn(column_t *column);


class Joiner {
    std::vector<Relation> relations; // The relations that might be joined

    public:

    /* 2 Jobs scheduler */
    JobScheduler job_scheduler1;
    JobScheduler job_scheduler2;


    /* Initialize the row_id Array */
    void RowIdArrayInit(QueryInfo &query_info);

    // Add relation
    void addRelation(const char* fileName);

    // Get relation
    Relation& getRelation(unsigned id);

    // Get the total number of relations
    int getRelationsCount();

    table_t*    CreateTableTFromId(unsigned rel_id, unsigned rel_binding);
    relation_t* CreateRowRelationT(uint64_t * column, unsigned size);
    relation_t* CreateRelationT(table_t * table, SelectInfo &sel_info);
    table_t*    CreateTableT(result_t * result, table_t * table_r, table_t * table_s, columnInfoMap & cmap);
    void        CheckSumOnTheFly(result_t * result, table_t * table_r, table_t * table_s, columnInfoMap & cmap, std::vector<SelectInfo> selections, string &);
    void        AddColumnToTableT(SelectInfo &sel_info, table_t *table);

    // The select functions
    void SelectAll(std::vector<FilterInfo*> & filterPtrs, table_t* table);
    void Select(FilterInfo &sel_info, table_t *table, ColumnInfo* columnInfo);
    void SelectEqual(table_t *table, int filter);
    void SelectGreater(table_t *table, int filter);
    void SelectLess(table_t *table, int filter);

    // Joins a given set of relations
    template <typename T1, typename T2, typename T3, typename T4, typename T5, typename T6, typename T7>
    T1* join(T1 *table_r, T1 *table_s, T2 &pred_info, T3 & cmap, T4 isRoot, std::vector<T5> & selections, T6 leafs, T7 & result_str){
        // #ifdef time
        // struct timeval start;
        // gettimeofday(&start, NULL);
        // #endif
        relation_t * r1;
        relation_t * r2;
        // #ifdef time
        // struct timeval end;
        // gettimeofday(&end, NULL);
        // timeCreateRelationT += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
        // gettimeofday(&start, NULL);
        // #endif
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

        result_t * res  = PRO(r1, r2, threads, c, job_scheduler1);

        if (leafs&1) {
            free(r1);
            idxcache[left] = c.R;
        }
        if (leafs&2) {
            free(r2);
            idxcache[right] = c.S;
        }
        // #ifdef time
        // gettimeofday(&end, NULL);
        // timeRadixJoin += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
        // #endif
        table_t *temp = NULL;
        // On root dont create a resilting table just get the checksums
        if (isRoot) {
            CheckSumOnTheFly(res, table_r, table_s, cmap, selections, result_str);
        } else {
            temp = CreateTableT(res, table_r, table_s, cmap);
        }

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
    table_t* SelfJoin(table_t *table, PredicateInfo *pred_info, columnInfoMap & cmap);
    table_t* SelfJoinCheckSumOnTheFly(table_t *table, PredicateInfo *predicate_ptr, columnInfoMap & cmap, std::vector<SelectInfo> selections, string & result_str);

    //caching info
    std::map<Selection, cached_t*> idxcache;

    Cacheinf newcf() {
        Cacheinf c;
        size_t   threads = THREAD_NUM_1CPU; // + THREAD_NUM_2CPU;
        c.S = (cached_t *) calloc(threads, sizeof(cached_t));
        c.R = (cached_t *) calloc(threads, sizeof(cached_t));
        return c;
    }
};

#include "QueryPlan.hpp"

int cleanQuery(QueryInfo &);
