#pragma once

#include <cstdint>
#include <limits>
#include <iostream>
#include <map>
#include <unordered_map>
#include <set>
#include <vector>
#include <math.h>
#include "Joiner.hpp"
#include "Parser.hpp"

using namespace std;

/* Initial Cost Estimations for Join */
// steps (horizontally & vertically): 1000 11000 21000 31000 41000
const std::vector< std::vector<int> > smallSameRelJoin{
                            {271, 2571, 6567, 11396, 18273},
                            {233, 1708, 6030, 11315, 17764},
                            {232, 1936, 6028, 11440, 17508},
                            {230, 2231, 5380, 11370, 17693},
                            {220, 2372, 5486, 11067, 18003}
                            };

const std::vector< std::vector<int> > smallDiffRelJoin{
                            {233, 499, 987, 1358, 1787},
                            {518, 2061, 3943, 5587, 7748},
                            {867, 3612, 6487, 7767, 10141},
                            {1038, 5056, 8956, 11406, 14310},
                            {1969, 6571, 10869, 16290, 20290}
                            };

const std::vector< std::vector<int> > smallSameRelJoinRJ{
                            {197, 1473, 2703, 4853, 7733},
                            {187, 1082, 2517, 4595, 7530},
                            {188, 1131, 2491, 4527, 7480},
                            {194, 1230, 2652, 4606, 7624},
                            {184, 1347, 2465, 4431, 7577}
                            };

const std::vector< std::vector<int> > smallSameRelJoinCreateTableT{
                            {13, 1028, 3657, 6074, 9974},
                            {15, 555, 3304, 6312, 9873},
                            {15, 737, 3339, 6495, 9755},
                            {12, 940, 2601, 6348, 9833},
                            {13, 952, 2835, 6297, 9914}
                            };

const std::vector< std::vector<int> > smallDiffRelJoinRJ{
                            {165, 414, 789, 987, 1200},
                            {431, 1075, 1755, 2035, 3625},
                            {713, 1791, 2726, 3092, 4029},
                            {813, 2258, 3336, 4446, 5806},
                            {1379, 2960, 4307, 6771, 7801}
                            };

const std::vector< std::vector<int> > smallDiffRelJoinCreateTableT{
                            {6, 48, 138, 163, 321},
                            {49, 883, 2085, 3301, 3933},
                            {99, 1720, 3546, 4522, 5791},
                            {150, 2594, 5348, 6555, 8282},
                            {321, 3391, 6246, 9346, 11947}
                            };

// for < 10000 results, for < 100000 results, for < 1000000 results, for < 10000000 results respectively
const std::vector<int> smallSameRelJoinCreateTableTResults{
                            15, 955, 5000, 10000
                            };

// for < 10000 results, for < 100000 results, for < 1000000 results, for < 10000000 results respectively
const std::vector<int> smallDiffRelJoinCreateTableTResults{
                            400, 800, 5000, 10150
                            };

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
