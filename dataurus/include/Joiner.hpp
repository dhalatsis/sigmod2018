#pragma once
#include <vector>
#include <cstdint>
#include <string>
#include <map>
#include <unordered_map>
#include <sys/time.h>
#include <string.h>
#include "Relation.hpp"
#include "Parser.hpp"
#include "table_t.hpp"
#include "parallel_radix_join.h"
#include "tuple_buffer.h"
#include "prj_params.h"
//#include "job_scheduler.h"
#include "create_job.h"

/* THread pool Includes */
#include <algorithm>
#include <array>
#include <cstdio>
#include <iostream>
#include <thread>
/*----------------*/

//#define time
//#define prints
#define THREAD_NUM 4
#define THREAD_NUM_1CPU 4
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

    /* do the checksum */
    //std::string check_sum(SelectInfo &sel_info, table_t *table, threadpool11::Pool & p, std::array<std::future<uint64_t>, THREAD_NUM> & f);
    std::string check_sum(SelectInfo &sel_info, table_t *table);

    /* Initialize the row_id Array */
    void RowIdArrayInit(QueryInfo &query_info);

    // Add relation
    void addRelation(const char* fileName);

    // Get relation
    Relation& getRelation(unsigned id);

    // Get the total number of relations
    int getRelationsCount();

    table_t*    CreateTableTFromId(unsigned rel_id, unsigned rel_binding);
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
    void join(QueryInfo& i);
    table_t* join(table_t *table_r, table_t *table_s, PredicateInfo &pred_info, columnInfoMap & cmap, bool isRoot, std::vector<SelectInfo> selections, int, string &);
    table_t* SelfJoin(table_t *table, PredicateInfo *pred_info, columnInfoMap & cmap);

    void noConstructSelfJoin(table_t *table, PredicateInfo *predicate_ptr, std::vector<SelectInfo> & selections);

    //caching info
    std::map<Selection, cached_t*> idxcache;


    Cacheinf newcf() {
        Cacheinf c;
        c.S = (cached_t *) calloc(THREAD_NUM, sizeof(cached_t));
        c.R = (cached_t *) calloc(THREAD_NUM, sizeof(cached_t));
        return c;
    }
};

#include "QueryPlan.hpp"

int cleanQuery(QueryInfo &);
