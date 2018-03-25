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

/* THread pool Includes */
#include "threadpool11/threadpool11.hpp"
#include <algorithm>
#include <array>
#include <cstdio>
#include <iostream>
#include <mutex>
#include <thread>
/*----------------*/

#define time

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
        threadpool11::Pool pool;  // Create a threadPool


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

    table_t* CreateTableTFromId(unsigned rel_id, unsigned rel_binding);
    relation_t * CreateRelationT(table_t * table, SelectInfo &sel_info);
    table_t * CreateTableT(result_t * result, table_t * table_r, table_t * table_s, columnInfoMap & cmap);
    void AddColumnToTableT(SelectInfo &sel_info, table_t *table);

    // The select functions
    void Select(FilterInfo &sel_info, table_t *table);
    void SelectEqual(table_t *table, int filter);
    void SelectGreater(table_t *table, int filter);
    void SelectLess(table_t *table, int filter);

    // Joins a given set of relations
    void join(QueryInfo& i);
    table_t* join(table_t *table_r, table_t *table_s, PredicateInfo &pred_info, columnInfoMap & cmap);
    table_t* SelfJoin(table_t *table, PredicateInfo *pred_info, columnInfoMap & cmap);

    void noConstructSelfJoin(table_t *table, PredicateInfo *predicate_ptr, std::vector<SelectInfo> & selections);

    // nested loops joins
    std::vector<table_t*> getTablesFromTree(JTree* jTreePtr);
    void for_join(JTree* jTreePtr, std::vector<SelectInfo> selections);
    void for_2(table_t* table_a, table_t* table_b, std::unordered_map< uint64_t, std::vector<uint64_t> > columns);
    void for_3(table_t* table_a, table_t* table_b, table_t* table_c, std::unordered_map< uint64_t, std::vector<uint64_t> > columns);
    void for_4(table_t* table_a, table_t* table_b, table_t* table_c, table_t* table_d, std::unordered_map< uint64_t, std::vector<uint64_t> > columns);

};

#include "header.hpp"
#include "QueryPlan.hpp"

int cleanQuery(QueryInfo &);
