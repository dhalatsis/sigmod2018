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
    table_t* join(table_t *table_r, table_t *table_s, PredicateInfo &pred_info, columnInfoMap & cmap, bool isRoot, std::vector<SelectInfo> selections, int, string &);
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
