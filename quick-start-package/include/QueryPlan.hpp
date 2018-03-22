#pragma once

#include <cstdint>
#include <limits>
#include <iostream>
#include <map>
#include <unordered_map>
#include <set>
#include <math.h>
#include "Joiner.hpp"
#include "Parser.hpp"

using namespace std;

// Keeps the important info/statistics for every column
// needed to build the plan tree
struct ColumnInfo {
    uint64_t min;      // Value of the minimum element
    uint64_t max;      // Value of the maximum element
    uint64_t size;     // Total number of elements
    uint64_t distinct; // Number of distinct elements
    uint64_t n;        // The size of the domain
    double spread;     // The spread of the values in the domain

    unsigned counter; // Number of times the column appears in the query
    bool isSelectionColumn;

    // Prints a Column Info structure
    void print();
};

typedef map<SelectInfo, ColumnInfo> columnInfoMap;

// Join Tree's node
struct JoinTreeNode {
    unsigned nodeId;
    double treeCost; // An estimation of the total cost of the join tree

    JoinTreeNode* left;
    JoinTreeNode* right;
    JoinTreeNode* parent;

    PredicateInfo* predicatePtr;
    FilterInfo* filterPtr;
    ColumnInfo columnInfo;

    columnInfoMap usedColumnInfos; // Keeps track of all the columns of every relation to be used in the query

    // Estimates the new info of a node's column
    // after a filter predicate is applied to that column
    void estimateInfoAfterFilter(FilterInfo& filterInfo);

    // Updates the column info map
    void estimateInfoAfterFilterLess(FilterInfo& filterInfo);
    void estimateInfoAfterFilterGreater(FilterInfo& filterInfo);
    void estimateInfoAfterFilterEqual(FilterInfo& filterInfo);

    // Estimates the info of a node's column
    // after a join predicate is applied to its children
    void estimateInfoAfterJoin(PredicateInfo& predicateInfo);
    
    // Updates the column info map
    ColumnInfo estimateInfoAfterLeftDependentJoin(PredicateInfo& predicateInfo);
    ColumnInfo estimateInfoAfterRightDependentJoin(PredicateInfo& predicateInfo);
    ColumnInfo estimateInfoAfterIndependentJoin(PredicateInfo& predicateInfo);

    // Execute a Join Tree
    table_t* execute(JoinTreeNode* joinTreeNodePtr, Joiner& joiner, QueryInfo& queryInfo);

    // Estimates the cost of a given Plan Tree Node
    void cost(PredicateInfo& predicateInfo);

    void print(JoinTreeNode* joinTreeNodePtr);
};

// Join Tree data structure
struct JoinTree {
    JoinTreeNode* root;

    // Constructs a JoinTree from a set of relations
    JoinTree* build(QueryInfo& queryInfoPtr, ColumnInfo** columnInfos);

    // Merges two join trees
    JoinTree* CreateJoinTree(JoinTree* leftTree, JoinTree* rightTree, PredicateInfo& predicateInfo);

    // Merges the final optimal tree with a filter join predicate
    JoinTree* AddFilterJoin(JoinTree* leftTree, PredicateInfo* predicateInfo);

    // Returns the cost of a given JoinTree
    double getCost();

    // destructor
    void destrJoinTree();
};

// Query Plan data structure
struct QueryPlan {
    // Keeps the info of every column of every relation
    // Every row represents a relation
    // Every item of a row represents a column of the relation
    ColumnInfo** columnInfos;

    JoinTree* joinTreePtr; // The plan tree to execute

    // Build a query plan with the given info
    void build(QueryInfo& queryInfoPtr);

    // destructor
    void destrQueryPlan(Joiner& joiner);

    // Execute a query plan with the given info
    void execute(QueryInfo& queryInfoPtr);

    // Fills the columnInfo matrix with the data of every column
    void fillColumnInfo(Joiner& joiner);
};
