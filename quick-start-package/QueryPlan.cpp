#include <bitset>
#include <unordered_set>
#include <math.h>
#include "QueryPlan.hpp"

//#include "tbb/concurrent_unordered_set.h"

using namespace std;

//#define prints

void ColumnInfo::print() {
    cerr << "min:      " << this->min << endl;
    cerr << "max:      " << this->max << endl;
    cerr << "size:     " << this->size << endl;
    cerr << "distinct: " << this->distinct << endl;
    cerr << "n:        " << this->n << endl;
    cerr << "spread:   " << this->spread << endl << endl;
    flush(cerr);
}

// Estimates the new info of a node's column
// after a filter predicate is applied to that column
void JoinTreeNode::estimateInfoAfterFilter(FilterInfo& filterInfo) {
    if (filterInfo.comparison == FilterInfo::Comparison::Less) {
        this->estimateInfoAfterFilterLess(filterInfo);
    }
    else if (filterInfo.comparison == FilterInfo::Comparison::Greater) {
        this->estimateInfoAfterFilterGreater(filterInfo);
    }
    else if (filterInfo.comparison == FilterInfo::Comparison::Equal) {
        this->estimateInfoAfterFilterEqual(filterInfo);
    }
}

// Updates the column info map
void JoinTreeNode::estimateInfoAfterFilterLess(FilterInfo& filterInfo) {
    ColumnInfo oldColumnInfo = this->usedColumnInfos[filterInfo.filterColumn];

    // Update the info of the column on which the filter is applied
    ColumnInfo newColumnInfo;

    newColumnInfo.min      = oldColumnInfo.min;
    newColumnInfo.max      = filterInfo.constant;
    newColumnInfo.distinct = (uint64_t) (((double) (newColumnInfo.max - newColumnInfo.min)) / oldColumnInfo.spread);
    newColumnInfo.size     = newColumnInfo.distinct * (oldColumnInfo.size / oldColumnInfo.distinct);
    newColumnInfo.n        = newColumnInfo.max - newColumnInfo.min + 1;
    newColumnInfo.spread   = ((double) newColumnInfo.n) / ((double) newColumnInfo.distinct);
    newColumnInfo.counter  = oldColumnInfo.counter - 1;

    this->usedColumnInfos[filterInfo.filterColumn] = newColumnInfo;

    // Update the info of the other columns
    for (columnInfoMap::iterator it=this->usedColumnInfos.begin(); it != this->usedColumnInfos.end(); it++) {
        if (!(it->first == filterInfo.filterColumn)) {
            double base      = 1 - (((double) newColumnInfo.size) / ((double) oldColumnInfo.size));
            double exponent  = ((double) it->second.size) / ((double) it->second.distinct);
            double tempValue = pow(base, exponent);

            it->second.size     = newColumnInfo.size;
            it->second.distinct = (uint64_t) (((double) it->second.distinct) * (1 - tempValue));
            it->second.spread   = ((double)it->second.n) / ((double) it->second.distinct);
        }
    }

    // Check if this column is needed anymore
    if ((newColumnInfo.counter == 0) && (newColumnInfo.isSelectionColumn == false)) {
        this->usedColumnInfos.erase(filterInfo.filterColumn);
    }
}

// Updates the column info map
void JoinTreeNode::estimateInfoAfterFilterGreater(FilterInfo& filterInfo) {
    ColumnInfo oldColumnInfo = this->usedColumnInfos[filterInfo.filterColumn];

    //fprintf(stderr, "About to apply filter on column %d.%d\n", filterInfo.filterColumn.binding, filterInfo.filterColumn.colId);
    //oldColumnInfo.print();

    // Update the info of the column on which the filter is applied
    ColumnInfo newColumnInfo;

    newColumnInfo.min      = filterInfo.constant;
    newColumnInfo.max      = oldColumnInfo.max;
    newColumnInfo.distinct = (uint64_t) (((double) (newColumnInfo.max - newColumnInfo.min)) / oldColumnInfo.spread);
    newColumnInfo.size     = newColumnInfo.distinct * (oldColumnInfo.size / oldColumnInfo.distinct);
    newColumnInfo.n        = newColumnInfo.max - newColumnInfo.min + 1;
    newColumnInfo.spread   = ((double) newColumnInfo.n) / ((double) newColumnInfo.distinct);
    newColumnInfo.counter  = oldColumnInfo.counter - 1;

    //fprintf(stderr, "After the filter\n");
    //newColumnInfo.print();

    this->usedColumnInfos[filterInfo.filterColumn] = newColumnInfo;

    // Update the info of the other columns
    for (columnInfoMap::iterator it=this->usedColumnInfos.begin(); it != this->usedColumnInfos.end(); it++) {
        if (!(it->first == filterInfo.filterColumn)) {
            double base      = 1 - (((double) newColumnInfo.size) / ((double) oldColumnInfo.size));
            double exponent  = ((double) it->second.size) / ((double) it->second.distinct);
            double tempValue = pow(base, exponent);

            it->second.size     = newColumnInfo.size;
            it->second.distinct = (uint64_t) (((double) it->second.distinct) * (1 - tempValue));
            it->second.spread   = ((double) it->second.n) / ((double) it->second.distinct);

            //fprintf(stderr, "Update after filter column %d.%d\n", it->first.binding, it->first.colId);
            //it->second.print();
        }
    }

    // Check if this column is needed anymore
    if ((newColumnInfo.counter == 0) && (newColumnInfo.isSelectionColumn == false)) {
        this->usedColumnInfos.erase(filterInfo.filterColumn);
    }
}

// Updates the column info map
void JoinTreeNode::estimateInfoAfterFilterEqual(FilterInfo& filterInfo) {
    ColumnInfo oldColumnInfo = this->usedColumnInfos[filterInfo.filterColumn];

    //fprintf(stderr, "About to apply filter on column %d.%d\n", filterInfo.filterColumn.binding, filterInfo.filterColumn.colId);
    //oldColumnInfo.print();

    // Update the info of the column on which the filter is applied
    ColumnInfo newColumnInfo;

    newColumnInfo.min      = filterInfo.constant;
    newColumnInfo.max      = filterInfo.constant;
    newColumnInfo.distinct = 1;
    newColumnInfo.size     = oldColumnInfo.size / oldColumnInfo.distinct;
    newColumnInfo.n        = 1;
    newColumnInfo.spread   = 1;
    newColumnInfo.counter  = oldColumnInfo.counter - 1;

    //fprintf(stderr, "After the filter\n");
    //newColumnInfo.print();

    this->usedColumnInfos[filterInfo.filterColumn] = newColumnInfo;

    // Update the info of the other columns
    for (columnInfoMap::iterator it=this->usedColumnInfos.begin(); it != this->usedColumnInfos.end(); it++) {
        if (!(it->first == filterInfo.filterColumn)) {
            double base      = 1 - (((double) newColumnInfo.size) / ((double) oldColumnInfo.size));
            double exponent  = ((double) it->second.size) / ((double) it->second.distinct);
            double tempValue = pow(base, exponent);

            it->second.size     = newColumnInfo.size;
            it->second.distinct = ceil(it->second.distinct * (1 - tempValue));
            it->second.spread   = ((double) it->second.n) / ((double) it->second.distinct);

            //fprintf(stderr, "Update after filter column %d.%d\n", it->first.binding, it->first.colId);
            //it->second.print();
        }
    }

    // Check if this column is needed anymore
    if ((newColumnInfo.counter == 0) && (newColumnInfo.isSelectionColumn == false)) {
        this->usedColumnInfos.erase(filterInfo.filterColumn);
    }
}

// Estimates the info of a node's column
// after a join predicate is applied to its children
void JoinTreeNode::estimateInfoAfterJoin(PredicateInfo& predicateInfo) {
    // Get the column info of the columns to be joined from the children
    ColumnInfo* leftColumnInfo = &(this->left->usedColumnInfos[predicateInfo.left]);
    ColumnInfo* rightColumnInfo = &(this->right->usedColumnInfos[predicateInfo.right]);

    //----------------
    /*
    fprintf(stderr, "Before predicate %d.%d=%d.%d\n", predicateInfo.left.binding, predicateInfo.left.colId,
        predicateInfo.right.binding, predicateInfo.right.colId);
    fprintf(stderr, "%d.%d\n", predicateInfo.left.binding, predicateInfo.left.colId);
    leftColumnInfo->print();
    fprintf(stderr, "%d.%d\n", predicateInfo.right.binding, predicateInfo.right.colId);
    rightColumnInfo->print();
    */
    //----------------

    // Save the current min and max in case they change
    uint64_t oldLeftMin  = leftColumnInfo->min;
    uint64_t oldLeftMax  = leftColumnInfo->max;
    uint64_t oldRightMin = rightColumnInfo->min;
    uint64_t oldRightMax = rightColumnInfo->max;

    // If the domains are not the same apply a custom filter
    if ((oldLeftMin != oldRightMin) || (oldLeftMax != rightColumnInfo->max)) {
        // First apply the right filters to create the same domain on both columns
        uint64_t oldDistinct     = leftColumnInfo->distinct;
        leftColumnInfo->min      = max(leftColumnInfo->min, rightColumnInfo->min);
        leftColumnInfo->max      = min(leftColumnInfo->max, rightColumnInfo->max);
        leftColumnInfo->distinct = (uint64_t) (((double) (leftColumnInfo->max - leftColumnInfo->min)) / leftColumnInfo->spread);
        if (leftColumnInfo->distinct == 0) leftColumnInfo->distinct = 1;
        leftColumnInfo->size     = leftColumnInfo->distinct * (leftColumnInfo->size / oldDistinct);
        leftColumnInfo->n        = leftColumnInfo->max - leftColumnInfo->min + 1;
        leftColumnInfo->spread   = ((double) leftColumnInfo->n) / ((double) leftColumnInfo->distinct);

        oldDistinct               = rightColumnInfo->distinct;
        rightColumnInfo->min      = leftColumnInfo->min;
        rightColumnInfo->max      = leftColumnInfo->max;
        rightColumnInfo->distinct = (uint64_t) (((double) (rightColumnInfo->max - rightColumnInfo->min)) / rightColumnInfo->spread);
        if (rightColumnInfo->distinct == 0) rightColumnInfo->distinct = 1;
        rightColumnInfo->size     = rightColumnInfo->distinct * (rightColumnInfo->size / oldDistinct);
        rightColumnInfo->n        = rightColumnInfo->max - rightColumnInfo->min + 1;
        rightColumnInfo->spread   = ((double) rightColumnInfo->n) / ((double) rightColumnInfo->distinct);

        //---------------
        /*
        fprintf(stderr, "AFTER APPLYING A CUSTOM FILTER\n");
        fprintf(stderr, "%d.%d\n", predicateInfo.left.binding, predicateInfo.left.colId);
        leftColumnInfo->print();
        fprintf(stderr, "%d.%d\n", predicateInfo.right.binding, predicateInfo.right.colId);
        rightColumnInfo->print();
        */
        //---------------
    }

    if ((oldLeftMin >= oldRightMin) && (oldLeftMax <= rightColumnInfo->max)) {
        this->estimateInfoAfterLeftDependentJoin(predicateInfo);
    }
    /*
    else if ((rightColumnInfo->min >= leftColumnInfo->min) && (rightColumnInfo->max <= leftColumnInfo->max)) {
        this->estimateInfoAfterRightDependentJoin(predicateInfo);
    }*/
    else {
        this->estimateInfoAfterIndependentJoin(predicateInfo);
    }
}

// Updates the column info map
ColumnInfo JoinTreeNode::estimateInfoAfterLeftDependentJoin(PredicateInfo& predicateInfo) {
    ColumnInfo newLeftColumnInfo, newRightColumnInfo;

    // Get the info of the columns to be joined
    ColumnInfo oldLeftColumnInfo = this->left->usedColumnInfos[predicateInfo.left];
    ColumnInfo oldRightColumnInfo = this->right->usedColumnInfos[predicateInfo.right];

    //------------------
    /*
    fprintf(stderr, "Inside left join - before join\n");
    fprintf(stderr, "%d.%d\n", predicateInfo.left.binding, predicateInfo.left.colId);
    oldLeftColumnInfo.print();
    fprintf(stderr, "%d.%d\n", predicateInfo.right.binding, predicateInfo.right.colId);
    oldRightColumnInfo.print();
    */
    //------------------

    // Estimate the new info of the columns to be joined
    newLeftColumnInfo.min      = oldLeftColumnInfo.min;
    newLeftColumnInfo.max      = oldLeftColumnInfo.max;
    newLeftColumnInfo.size     = oldLeftColumnInfo.size * (oldRightColumnInfo.size / oldRightColumnInfo.distinct);
    newLeftColumnInfo.distinct = oldLeftColumnInfo.distinct;
    newLeftColumnInfo.n        = newLeftColumnInfo.max - newLeftColumnInfo.min + 1;
    newLeftColumnInfo.spread   = ((double) newLeftColumnInfo.n) / ((double) newLeftColumnInfo.distinct);
    newLeftColumnInfo.counter  = oldLeftColumnInfo.counter - 1;

    newRightColumnInfo.min      = oldRightColumnInfo.min;
    newRightColumnInfo.max      = oldRightColumnInfo.max;
    newRightColumnInfo.size     = oldLeftColumnInfo.size * (oldRightColumnInfo.size / oldRightColumnInfo.distinct);
    newRightColumnInfo.distinct = oldLeftColumnInfo.distinct;
    newRightColumnInfo.n        = newRightColumnInfo.max - newRightColumnInfo.min + 1;
    newRightColumnInfo.spread   = ((double) newRightColumnInfo.n) / ((double) newRightColumnInfo.distinct);
    newRightColumnInfo.counter  = oldRightColumnInfo.counter - 1;

    //------------------
    /*
    fprintf(stderr, "Inside left join - after join\n");
    fprintf(stderr, "%d.%d\n", predicateInfo.left.binding, predicateInfo.left.colId);
    newLeftColumnInfo.print();
    fprintf(stderr, "%d.%d\n", predicateInfo.right.binding, predicateInfo.right.colId);
    newRightColumnInfo.print();
    */
    //------------------

    this->usedColumnInfos[predicateInfo.left] = newLeftColumnInfo;
    this->usedColumnInfos[predicateInfo.right] = newRightColumnInfo;

    // Update the info of the other columns
    // in the same relation as the left predicate
    for (columnInfoMap::iterator it=this->left->usedColumnInfos.begin(); it != this->left->usedColumnInfos.end(); it++) {
        if ((!(it->first == predicateInfo.left)) && (it->first.binding == predicateInfo.left.binding)) {
            double base      = 1 - (((double) newLeftColumnInfo.size) / (((double) oldLeftColumnInfo.size) * ((double) oldRightColumnInfo.size)));
            double exponent  = ((double) it->second.size) / ((double) it->second.distinct);
            double tempValue = pow(base, exponent);

            it->second.size     = newLeftColumnInfo.size;
            it->second.distinct = ceil(it->second.distinct * (1 - tempValue));
            it->second.spread   = ((double) it->second.n) / ((double) it->second.distinct);
            this->usedColumnInfos[it->first] = it->second;

            //fprintf(stderr, "Update after join column %d.%d\n", it->first.binding, it->first.colId);
            //it->second.print();
        }
    }

    // Update the info of the other columns
    // in the same relation as the right predicate
    for (columnInfoMap::iterator it=this->right->usedColumnInfos.begin(); it != this->right->usedColumnInfos.end(); it++) {
        if ((!(it->first == predicateInfo.right)) && (it->first.binding == predicateInfo.right.binding)) {
            double base      = 1 - (((double) newRightColumnInfo.size) / (((double) oldLeftColumnInfo.size) * ((double) oldRightColumnInfo.size)));
            double exponent  = ((double) it->second.size) / ((double) it->second.distinct);
            double tempValue = pow(base, exponent);

            it->second.size     = newRightColumnInfo.size;
            it->second.distinct = ceil(it->second.distinct * (1 - tempValue));
            it->second.spread   = ((double) it->second.n) / ((double) it->second.distinct);
            this->usedColumnInfos[it->first] = it->second;

            //fprintf(stderr, "Update after join column %d.%d\n", it->first.binding, it->first.colId);
            //it->second.print();
        }
    }

    // Copy every other remaining column
    for (columnInfoMap::iterator it=this->left->usedColumnInfos.begin(); it != this->left->usedColumnInfos.end(); it++) {
        if (it->first.binding != predicateInfo.left.binding) {
            this->usedColumnInfos[it->first] = it->second;
        }
    }

    for (columnInfoMap::iterator it=this->right->usedColumnInfos.begin(); it != this->right->usedColumnInfos.end(); it++) {
        if (it->first.binding != predicateInfo.right.binding) {
            this->usedColumnInfos[it->first] = it->second;
        }
    }

    // Check if the left column is needed anymore
    if ((newLeftColumnInfo.counter == 0) && (newLeftColumnInfo.isSelectionColumn == false)) {
        this->usedColumnInfos.erase(predicateInfo.left);
    }

    // Check if the right column is needed anymore
    if ((newRightColumnInfo.counter == 0) && (newRightColumnInfo.isSelectionColumn == false)) {
        this->usedColumnInfos.erase(predicateInfo.right);
    }
}

// Updates the column info map
ColumnInfo JoinTreeNode::estimateInfoAfterRightDependentJoin(PredicateInfo& predicateInfo) {
    ColumnInfo newLeftColumnInfo, newRightColumnInfo;

    // Get the info of the columns to be joined
    ColumnInfo oldLeftColumnInfo = this->left->usedColumnInfos[predicateInfo.left];
    ColumnInfo oldRightColumnInfo = this->right->usedColumnInfos[predicateInfo.right];

    // Estimate the new info of the columns to be joined
    newLeftColumnInfo.min      = oldLeftColumnInfo.min;
    newLeftColumnInfo.max      = oldLeftColumnInfo.max;
    newLeftColumnInfo.size     = oldRightColumnInfo.size * (oldLeftColumnInfo.size / oldLeftColumnInfo.distinct);
    newLeftColumnInfo.distinct = oldRightColumnInfo.distinct;
    newLeftColumnInfo.n        = newLeftColumnInfo.max - newLeftColumnInfo.min + 1;
    newLeftColumnInfo.spread   = ((double) newLeftColumnInfo.n) / ((double) newLeftColumnInfo.distinct);
    newLeftColumnInfo.counter  = oldLeftColumnInfo.counter - 1;

    newRightColumnInfo.min      = oldRightColumnInfo.min;
    newRightColumnInfo.max      = oldRightColumnInfo.max;
    newRightColumnInfo.size     = oldRightColumnInfo.size * (oldLeftColumnInfo.size / oldLeftColumnInfo.distinct);
    newRightColumnInfo.distinct = oldRightColumnInfo.distinct;
    newRightColumnInfo.n        = newRightColumnInfo.max - newRightColumnInfo.min + 1;
    newRightColumnInfo.spread   = ((double) newRightColumnInfo.n) / ((double) newRightColumnInfo.distinct);
    newRightColumnInfo.counter  = oldRightColumnInfo.counter - 1;

    this->usedColumnInfos[predicateInfo.left] = newLeftColumnInfo;
    this->usedColumnInfos[predicateInfo.right] = newRightColumnInfo;

    // Update the info of the other columns
    // in the same relation as the left predicate
    for (columnInfoMap::iterator it=this->left->usedColumnInfos.begin(); it != this->left->usedColumnInfos.end(); it++) {
        if ((!(it->first == predicateInfo.left)) && (it->first.binding == predicateInfo.left.binding)) {
            double base      = 1 - (((double) newLeftColumnInfo.size) / (((double) oldLeftColumnInfo.size) * ((double) oldRightColumnInfo.size)));
            double exponent  = ((double) it->second.size) / ((double) it->second.distinct);
            double tempValue = pow(base, exponent);

            it->second.size     = newLeftColumnInfo.size;
            it->second.distinct = ceil(it->second.distinct * (1 - tempValue));
            it->second.spread   = ((double) it->second.n) / ((double) it->second.distinct);
            this->usedColumnInfos[it->first] = it->second;
        }
    }

    // Update the info of the other columns
    // in the same relation as the right predicate
    for (columnInfoMap::iterator it=this->right->usedColumnInfos.begin(); it != this->right->usedColumnInfos.end(); it++) {
        if ((!(it->first == predicateInfo.right)) && (it->first.binding == predicateInfo.right.binding)) {
            double base      = 1 - (((double) newRightColumnInfo.size) / (((double) oldLeftColumnInfo.size) * ((double) oldRightColumnInfo.size)));
            double exponent  = ((double) it->second.size) / ((double) it->second.distinct);
            double tempValue = pow(base, exponent);

            it->second.size     = newRightColumnInfo.size;
            it->second.distinct = ceil(it->second.distinct * (1 - tempValue));
            it->second.spread   = ((double) it->second.n) / ((double) it->second.distinct);
            this->usedColumnInfos[it->first] = it->second;
        }
    }

    // Copy every other remaining column
    for (columnInfoMap::iterator it=this->left->usedColumnInfos.begin(); it != this->left->usedColumnInfos.end(); it++) {
        if (it->first.binding != predicateInfo.left.binding) {
            this->usedColumnInfos[it->first] = it->second;
        }
    }

    for (columnInfoMap::iterator it=this->right->usedColumnInfos.begin(); it != this->right->usedColumnInfos.end(); it++) {
        if (it->first.binding != predicateInfo.right.binding) {
            this->usedColumnInfos[it->first] = it->second;
        }
    }

    // Check if the left column is needed anymore
    if ((newLeftColumnInfo.counter == 0) && (newLeftColumnInfo.isSelectionColumn == false)) {
        this->usedColumnInfos.erase(predicateInfo.left);
    }

    // Check if the right column is needed anymore
    if ((newRightColumnInfo.counter == 0) && (newRightColumnInfo.isSelectionColumn == false)) {
        this->usedColumnInfos.erase(predicateInfo.right);
    }
}

// Updates the column info map
ColumnInfo JoinTreeNode::estimateInfoAfterIndependentJoin(PredicateInfo& predicateInfo) {
    ColumnInfo newLeftColumnInfo, newRightColumnInfo;

    // Get the info of the columns to be joined
    ColumnInfo oldLeftColumnInfo = this->left->usedColumnInfos[predicateInfo.left];
    ColumnInfo oldRightColumnInfo = this->right->usedColumnInfos[predicateInfo.right];

    // Estimate the new info of the columns to be joined
    newLeftColumnInfo.min      = oldLeftColumnInfo.min;
    newLeftColumnInfo.max      = oldLeftColumnInfo.max;
    newLeftColumnInfo.size     = (oldLeftColumnInfo.size * oldRightColumnInfo.size);
    newLeftColumnInfo.distinct = oldRightColumnInfo.distinct;
    newLeftColumnInfo.n        = newLeftColumnInfo.max - newLeftColumnInfo.min + 1;
    newLeftColumnInfo.spread   = ((double) newLeftColumnInfo.n) / ((double) newLeftColumnInfo.distinct);
    newLeftColumnInfo.counter  = oldLeftColumnInfo.counter - 1;

    newRightColumnInfo.min      = oldRightColumnInfo.min;
    newRightColumnInfo.max      = oldRightColumnInfo.max;
    newRightColumnInfo.size     = oldRightColumnInfo.size * (oldLeftColumnInfo.size / oldLeftColumnInfo.distinct);
    newRightColumnInfo.distinct = oldRightColumnInfo.distinct;
    newRightColumnInfo.n        = newRightColumnInfo.max - newRightColumnInfo.min + 1;
    newRightColumnInfo.spread   = ((double) newRightColumnInfo.n) / ((double) newRightColumnInfo.distinct);
    newRightColumnInfo.counter  = oldRightColumnInfo.counter - 1;

    this->usedColumnInfos[predicateInfo.left] = newLeftColumnInfo;
    this->usedColumnInfos[predicateInfo.right] = newRightColumnInfo;

    // Update the info of the other columns
    // in the same relation as the left predicate
    for (columnInfoMap::iterator it=this->left->usedColumnInfos.begin(); it != this->left->usedColumnInfos.end(); it++) {
        if ((!(it->first == predicateInfo.left)) && (it->first.binding == predicateInfo.left.binding)) {
            double base      = 1 - (((double) newLeftColumnInfo.size) / (((double) oldLeftColumnInfo.size) * ((double) oldRightColumnInfo.size)));
            double exponent  = ((double) it->second.size) / ((double) it->second.distinct);
            double tempValue = pow(base, exponent);

            it->second.size     = newLeftColumnInfo.size;
            it->second.distinct = ceil(it->second.distinct * (1 - tempValue));
            it->second.spread   = ((double) it->second.n) / ((double) it->second.distinct);
            this->usedColumnInfos[it->first] = it->second;
        }
    }

    // Update the info of the other columns
    // in the same relation as the right predicate
    for (columnInfoMap::iterator it=this->right->usedColumnInfos.begin(); it != this->right->usedColumnInfos.end(); it++) {
        if ((!(it->first == predicateInfo.right)) && (it->first.binding == predicateInfo.right.binding)) {
            double base      = 1 - (((double) newRightColumnInfo.size) / (((double) oldLeftColumnInfo.size) * ((double) oldRightColumnInfo.size)));
            double exponent  = ((double) it->second.size) / ((double) it->second.distinct);
            double tempValue = pow(base, exponent);

            it->second.size     = newRightColumnInfo.size;
            it->second.distinct = ceil(it->second.distinct * (1 - tempValue));
            it->second.spread   = ((double) it->second.n) / ((double) it->second.distinct);
            this->usedColumnInfos[it->first] = it->second;
        }
    }

    // Copy every other remaining column
    for (columnInfoMap::iterator it=this->left->usedColumnInfos.begin(); it != this->left->usedColumnInfos.end(); it++) {
        if (it->first.binding != predicateInfo.left.binding) {
            this->usedColumnInfos[it->first] = it->second;
        }
    }

    for (columnInfoMap::iterator it=this->right->usedColumnInfos.begin(); it != this->right->usedColumnInfos.end(); it++) {
        if (it->first.binding != predicateInfo.right.binding) {
            this->usedColumnInfos[it->first] = it->second;
        }
    }

    // Check if the left column is needed anymore
    if ((newLeftColumnInfo.counter == 0) && (newLeftColumnInfo.isSelectionColumn == false)) {
        this->usedColumnInfos.erase(predicateInfo.left);
    }

    // Check if the right column is needed anymore
    if ((newRightColumnInfo.counter == 0) && (newRightColumnInfo.isSelectionColumn == false)) {
        this->usedColumnInfos.erase(predicateInfo.right);
    }
}

// Construct a JoinTree from a set of relations
JoinTree* JoinTree::build(QueryInfo& queryInfo, ColumnInfo** columnInfos) {
    // Maps every possible set of relations to its respective best plan tree
    unordered_map< vector<bool>, JoinTree* > BestTree;
    int relationsCount = queryInfo.relationIds.size();

    // Initialise the BestTree structure with nodes
    // for every single relation in the input
    for (int i = 0; i < relationsCount; i++) {
        // Allocate memory
        JoinTree* joinTreePtr = new JoinTree();
        JoinTreeNode* joinTreeNodePtr = new JoinTreeNode();

        // Initialise JoinTreeNode
        joinTreeNodePtr->nodeId = i; // The binding of the relation
        joinTreeNodePtr->treeCost = 0;
        joinTreeNodePtr->left = NULL;
        joinTreeNodePtr->right = NULL;
        joinTreeNodePtr->parent = NULL;
        joinTreeNodePtr->predicatePtr = NULL;

        // Save the initial info of all the columns to be used in the query
        unsigned relationId, columnId;

        for (auto predicate : queryInfo.predicates) {
            if (predicate.left.binding == i) {
                // If the column is not already in the map insert it's info
                if (joinTreeNodePtr->usedColumnInfos.find(predicate.left) == joinTreeNodePtr->usedColumnInfos.end()) {
                    relationId = predicate.left.relId;
                    columnId = predicate.left.colId;
                    joinTreeNodePtr->usedColumnInfos[predicate.left] = columnInfos[relationId][columnId];
                }

                joinTreeNodePtr->usedColumnInfos[predicate.left].counter++;
            }
            else if (predicate.right.binding == i) {
                // If the column is not already in the map insert it's info
                if (joinTreeNodePtr->usedColumnInfos.find(predicate.right) == joinTreeNodePtr->usedColumnInfos.end()) {
                    relationId = predicate.right.relId;
                    columnId = predicate.right.colId;
                    joinTreeNodePtr->usedColumnInfos[predicate.right] = columnInfos[relationId][columnId];
                }

                joinTreeNodePtr->usedColumnInfos[predicate.right].counter++;
            }
        }

        for (auto filter : queryInfo.filters) {
            if (filter.filterColumn.binding == i) {
                // If the column is not already in the map insert it's info
                if (joinTreeNodePtr->usedColumnInfos.find(filter.filterColumn) == joinTreeNodePtr->usedColumnInfos.end()) {
                    relationId = filter.filterColumn.relId;
                    columnId = filter.filterColumn.colId;
                    joinTreeNodePtr->usedColumnInfos[filter.filterColumn] = columnInfos[relationId][columnId];
                }

                joinTreeNodePtr->usedColumnInfos[filter.filterColumn].counter++;
            }
        }

        for (auto selection : queryInfo.selections) {
            if (selection.binding == i) {
                // If the column is not already in the map insert it's info
                if (joinTreeNodePtr->usedColumnInfos.find(selection) == joinTreeNodePtr->usedColumnInfos.end()) {
                    relationId = selection.relId;
                    columnId = selection.colId;
                    joinTreeNodePtr->usedColumnInfos[selection] = columnInfos[relationId][columnId];
                }

                joinTreeNodePtr->usedColumnInfos[selection].counter++;
                joinTreeNodePtr->usedColumnInfos[selection].isSelectionColumn = true;
            }
        }

        // Initialise JoinTree
        joinTreePtr->root = joinTreeNodePtr;

        // Insert into the BestTree
        vector<bool> relationToVector(relationsCount, false);
        relationToVector[i] = true;
        BestTree[relationToVector] = joinTreePtr;
    }

    // Maps all sets of a certain size to their size
    // Sets are represented as vector<bool>
    map<int, set<vector<bool> > > powerSetMap;

    // Generate power-set of the given set of relations
    // source: www.geeksforgeeks.org/power-set/
    unsigned int powerSetSize = pow(2, relationsCount);

    for (int counter = 0; counter < powerSetSize; counter++) {
        vector<bool> tempVec(relationsCount, false);
        int setSize = 0;

        for (int j = 0; j < relationsCount; j++) {
            if (counter & (1 << j)) {
                tempVec[j] = true;
                setSize++;
            }

            // Save all sets of a certain size
            powerSetMap[setSize].insert(tempVec);
        }
    }

    // Apply all the filters first
    for (int i=0; i < queryInfo.filters.size(); i++) {
        // Update the tree (containing a single node)
        // of the relation whose column will be filtered
        vector<bool> relationToVector(relationsCount, false);
        relationToVector[queryInfo.filters[i].filterColumn.binding] = true;
        BestTree[relationToVector]->root->filterPtrs.push_back(&(queryInfo.filters[i]));

        // Update the column info
        BestTree[relationToVector]->root->estimateInfoAfterFilter(queryInfo.filters[i]);
    }

    // Dynamic programming algorithm
    for (int i = 1; i < relationsCount; i++) {
        for (auto s : powerSetMap[i]) {
            for (int j = 0; j < relationsCount; j++) {
                // If j is not in the set
                if (s[j] == false) {
                    // Check if there is a corresponding predicate
                    for (auto predicate : queryInfo.predicates) {
                        // If the right relation is found on the right hand side of a predicate
                        if (predicate.right.binding == j) {
                            for (int n = 0; n < relationsCount; n++) {
                                // If a relation from the set is found on the left hand side of the same predicate
                                if ((s[n] == true) && (predicate.left.binding == n)) {
                                    // Create the bit vector representation of the relation j
                                    vector<bool> relationToVector(relationsCount, false);
                                    relationToVector[j] = true;

                                    // If no predicate exists for the relations in the set
                                    // a tree has not been created
                                    if (BestTree[s] == NULL) continue;

                                    // Merge the two trees
                                    JoinTree* currTree = CreateJoinTree(BestTree[s], BestTree[relationToVector], predicate);

                                    // Save the new merged tree
                                    vector<bool> s1 = s;
                                    s1[j] = true;

                                    if ((BestTree[s1] == NULL) || (BestTree[s1]->getCost() > currTree->getCost())) {
                                        BestTree[s1] = currTree;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // Create a set of the predicates
    set<PredicateInfo*> predicatesSet;
    for (int i = 0; i < queryInfo.predicates.size(); i++) {
        predicatesSet.insert(&(queryInfo.predicates[i]));
    }

    // Set the predicates for every node
    vector<bool> rootToVector(relationsCount, true);
    JoinTreeNode* joinTreeNodePtr = BestTree[rootToVector]->root;
    set<int> joinedNodes; // Keep the bindings of the already joined nodes

    // Go to the leftmost join
    while (joinTreeNodePtr->left->nodeId == -1) {
        JoinTreeNode* temp = joinTreeNodePtr;
        joinTreeNodePtr = joinTreeNodePtr->left;
        joinTreeNodePtr->parent = temp;
    }

    // Go bottom-up and save the corresponding predicates
    joinedNodes.insert(joinTreeNodePtr->left->nodeId);

    while (1) {
        bool predicateFound = false;
        for (auto predicate : predicatesSet) {
            // If the right relation is found on the right hand side of a predicate
            if (predicate->right.binding == joinTreeNodePtr->right->nodeId) {

                for (auto n : joinedNodes) {
                    if (predicate->left.binding == n) {
                        joinTreeNodePtr->predicatePtr = predicate;
                        predicatesSet.erase(predicate);
                        joinedNodes.insert(joinTreeNodePtr->right->nodeId);
                        predicateFound = true;
                        break;
                    }
                }
            }

            if (predicateFound == true) break;
        }

        // Go to parent
        if (joinTreeNodePtr->parent != NULL) {
            joinTreeNodePtr = joinTreeNodePtr->parent;
        }
        else {
            break;
        }
    }

    // Return the optimal tree
    while (predicatesSet.size() != 0) {
        // Merge the self joins with the root
        BestTree[rootToVector] = AddFilterJoin(BestTree[rootToVector], *(predicatesSet.begin()));
        predicatesSet.erase(predicatesSet.begin());
    }

    return BestTree[rootToVector];
}

// Merges two join trees
JoinTree* JoinTree::CreateJoinTree(JoinTree* leftTree, JoinTree* rightTree, PredicateInfo& predicateInfo) {
    // Allocate memory for the new tree
    JoinTree* joinTreePtr = new JoinTree();
    JoinTreeNode* joinTreeNodePtr = new JoinTreeNode();

    // Initialise the new JoinTreeNode
    joinTreeNodePtr->nodeId = -1; // This is an intermediate node
    joinTreeNodePtr->treeCost = 0;
    joinTreeNodePtr->left = leftTree->root;
    joinTreeNodePtr->right = rightTree->root;
    joinTreeNodePtr->parent = NULL;
    joinTreeNodePtr->predicatePtr = NULL;

    // Assign a cost to this node
    joinTreeNodePtr->cost(predicateInfo);

    // Estimate the new info of the merged columns
    joinTreeNodePtr->estimateInfoAfterJoin(predicateInfo);

    // Initialise the new JoinTree
    joinTreePtr->root = joinTreeNodePtr;

    // Update the parent pointers of the merged trees
    //leftTree->root->parent = joinTreePtr->root;
    //rightTree->root->parent = joinTreePtr->root;

    return joinTreePtr;
}

// Merges the final optimal tree with a filter join predicate
JoinTree* JoinTree::AddFilterJoin(JoinTree* leftTree, PredicateInfo* predicateInfo) {
    // Allocate memory for the new tree
    JoinTree* joinTreePtr = new JoinTree();
    JoinTreeNode* joinTreeNodePtr = new JoinTreeNode();

    // Initialise the new JoinTreeNode
    joinTreeNodePtr->nodeId = -1; // This is an intermediate node
    joinTreeNodePtr->treeCost = 0;
    joinTreeNodePtr->left = leftTree->root;
    joinTreeNodePtr->right = NULL;
    joinTreeNodePtr->parent = NULL;
    joinTreeNodePtr->predicatePtr = predicateInfo;
    joinTreeNodePtr->usedColumnInfos = joinTreeNodePtr->left->usedColumnInfos;

    ColumnInfo *leftColumnInfo, *rightColumnInfo;
    leftColumnInfo = &(joinTreeNodePtr->usedColumnInfos[predicateInfo->left]);
    rightColumnInfo = &(joinTreeNodePtr->usedColumnInfos[predicateInfo->right]);

    // Check if the left column is needed anymore
    leftColumnInfo->counter--;
    if ((leftColumnInfo->counter == 0) && (leftColumnInfo->isSelectionColumn == false)) {
        joinTreeNodePtr->usedColumnInfos.erase(predicateInfo->left);
    }

    // Check if the right column is needed anymore
    rightColumnInfo->counter--;
    if ((rightColumnInfo->counter == 0) && (rightColumnInfo->isSelectionColumn == false)) {
        joinTreeNodePtr->usedColumnInfos.erase(predicateInfo->right);
    }

    // Initialise the new JoinTree
    joinTreePtr->root = joinTreeNodePtr;

    // Update the parent pointers of the merged trees
    leftTree->root->parent = joinTreePtr->root;
    return joinTreePtr;
}

//#define prints

// execute the plan described by a Plan Tree
table_t* JoinTreeNode::execute(JoinTreeNode* joinTreeNodePtr, Joiner& joiner, QueryInfo& queryInfo) {
    JoinTreeNode *left  = joinTreeNodePtr->left;
    JoinTreeNode *right = joinTreeNodePtr->right;
    table_t *table_l;
    table_t *table_r;
    table_t *res;

    // Leaf node containing a single relation
    if (left == NULL && right == NULL) {
        res = joiner.CreateTableTFromId(queryInfo.relationIds[joinTreeNodePtr->nodeId], joinTreeNodePtr->nodeId);

        // Apply the filter
        // Apply the filter
        for (auto filter : joinTreeNodePtr->filterPtrs) {
            joiner.AddColumnToTableT(filter->filterColumn, res);
            joiner.Select(*filter, res);
            #ifdef prints
            std::cerr << "----Filter Predicates: " <<  '\n';
            std::cerr << "Relation.column: "  << filter->filterColumn.relId << "." << filter->filterColumn.colId << '\n';
            std::cerr << "Constant: " << filter->constant << '\n';
            std::cerr << "Intermediate rows: " << res->tups_num << '\n';
            std::cerr << "-------" << '\n';
            #endif
        }
        return res;
    }

    // Go left
    table_l = joinTreeNodePtr->execute(left, joiner, queryInfo);

    // This is an intermediate node (join)
    if (right != NULL) {
        table_r = joinTreeNodePtr->execute(right, joiner, queryInfo);
        joiner.AddColumnToTableT(joinTreeNodePtr->predicatePtr->left, table_l);
        joiner.AddColumnToTableT(joinTreeNodePtr->predicatePtr->right, table_r);

        #ifdef prints
        std::cerr << "++++JOIN Predicates: " <<  '\n';
        std::cerr << "Left: "  << joinTreeNodePtr->predicatePtr->left.relId << "." << joinTreeNodePtr->predicatePtr->left.colId << " Size " << table_l->tups_num << '\n';
        std::cerr << "Right: " << joinTreeNodePtr->predicatePtr->right.relId << "." << joinTreeNodePtr->predicatePtr->right.colId << " Size " << table_r->tups_num << '\n';
        #endif
        res = joiner.join(table_l, table_r, *joinTreeNodePtr->predicatePtr, joinTreeNodePtr->usedColumnInfos);
        #ifdef prints
        std::cerr << "Intermediate rows: " << res->tups_num << '\n';
        std::cerr << "-------" << '\n';
        #endif
        return res;

    }
    else {
        #ifdef prints
        std::cerr << "======SELF Predicates: " <<  '\n';
        std::cerr << "Left: "  << joinTreeNodePtr->predicatePtr->left.relId << "." << joinTreeNodePtr->predicatePtr->left.colId << " Size " << table_l->tups_num << '\n';
        std::cerr << "Right: " << joinTreeNodePtr->predicatePtr->right.relId << "." << joinTreeNodePtr->predicatePtr->right.colId << " Size " << table_l->tups_num << '\n';
        #endif
        res = joiner.SelfJoin(table_l, joinTreeNodePtr->predicatePtr, joinTreeNodePtr->usedColumnInfos);
        #ifdef prints
        std::cerr << "Intermediate rows: " << res->tups_num << '\n';
        std::cerr << "-------" << '\n';
        #endif
        return res;
    }
}

// Estimate the cost of a JoinTreeNode
void JoinTreeNode::cost(PredicateInfo& predicateInfo) {
    this->treeCost = this->left->treeCost + this->usedColumnInfos[predicateInfo.left].size;
/*
    unsigned joinRowIndex = (this->left->usedColumnInfos[predicateInfo.left].size + 1000) / 10000;
    unsigned joinColIndex = (this->right->usedColumnInfos[predicateInfo.right].size + 1000) / 10000;

    unsigned constructIndex;
    if (this->usedColumnInfos[predicateInfo.left].size < 10000) constructIndex = 0;
    else if (this->usedColumnInfos[predicateInfo.left].size < 100000) constructIndex = 1;
    else if (this->usedColumnInfos[predicateInfo.left].size < 1000000) constructIndex = 2;
    else constructIndex = 3;

    fprintf(stderr, "left size       = %lu\n", this->left->usedColumnInfos[predicateInfo.left].size);
    fprintf(stderr, "right size      = %lu\n", this->right->usedColumnInfos[predicateInfo.right].size);
    fprintf(stderr, "join row index  = %d\n", joinRowIndex);
    fprintf(stderr, "join col index  = %d\n", joinColIndex);
    fprintf(stderr, "result size     = %lu\n", this->usedColumnInfos[predicateInfo.left].size);
    fprintf(stderr, "construct index = %d\n\n", constructIndex);
*/
    //this->treeCost = this->left->treeCost + joinCost + constructCost;
}

// Returns the cost of a given JoinTree
double JoinTree::getCost() {
    return this->root->treeCost;
}
/*
// Estimates the cost of a given Plan Tree
double JoinTreeNode::cost() {
    double nodeCostEstimation = 1.0;

    // if it is a leaf or a filter
    if ((this->filterPtr == NULL && this->predicatePtr == NULL) || this->filterPtr != NULL)
        nodeCostEstimation = 0;
    // if it is a join
    else if (this->predicatePtr != NULL && this->left != NULL && this->right != NULL) {
        // int i = this->left->columnInfo.size / 10000, j = this->right->columnInfo.size / 10000;
        int i = -1, j = -1, offset = 1;
        while (!(i >= 0 && i < 5 && j >= 0 && j < 5)) {
            i = this->left->columnInfo.size / (10000 * offset);
            j = this->right->columnInfo.size / (10000 * offset);
            offset *= 10;
        }
        nodeCostEstimation = smallDiffRelJoin[i][j] * offset;
        // if it is a self join
        if (this->left->nodeId != -1 && this->left->nodeId == this->right->nodeId){
            nodeCostEstimation = smallSameRelJoin[i][j] * offset;
            // nodeCostEstimation += (this->left->columnInfo.size * this->left->columnInfo.size) / this->left->columnInfo.distinct;
        }
        // // if left relation may be a subset of the right
        // else if ((this->left->columnInfo.min >= this->right->columnInfo.min) &&
        // (this->left->columnInfo.max <= this->right->columnInfo.max))
        //     nodeCostEstimation += (this->left->columnInfo.size * this->right->columnInfo.size) / this->right->columnInfo.distinct;
        // // if right relation may be a subset of the right
        // else if ((this->left->columnInfo.min <= this->right->columnInfo.min) &&
        // (this->left->columnInfo.max >= this->right->columnInfo.max))
        //     nodeCostEstimation += (this->left->columnInfo.size * this->right->columnInfo.size) / this->left->columnInfo.distinct;
        // // if the columns may be independent
        // else
        //     nodeCostEstimation += (this->left->columnInfo.size * this->right->columnInfo.size) / this->left->columnInfo.n;
    }

    if (this->left != NULL && this->right != NULL)
        nodeCostEstimation += this->left->cost() + this->right->cost();
    else if (this->left != NULL)
        nodeCostEstimation += this->left->cost();
    else if (this->right != NULL)
        nodeCostEstimation += this->right->cost();

    return nodeCostEstimation;
}

// Estimates the cost of a given Plan Tree
double JoinTree::cost(JoinTree* joinTreePtr) {
    if (joinTreePtr != NULL && joinTreePtr->root != NULL)
       return joinTreePtr->root->cost();
    // return 1.0;
}
*/
void JoinTreeNode::print(JoinTreeNode* joinTreeNodePtr) {
    if (joinTreeNodePtr == NULL) {
        return;
    }

    int depth = 0;

    while (joinTreeNodePtr->nodeId == -1) {
        for (int i=0; i < depth; i++) fprintf(stderr,"    ");
        fprintf(stderr, "In node with predicate: ");
        fprintf(stderr,"%d.%d=%d.%d\n", joinTreeNodePtr->predicatePtr->left.binding,
            joinTreeNodePtr->predicatePtr->left.colId, joinTreeNodePtr->predicatePtr->right.binding,
            joinTreeNodePtr->predicatePtr->right.colId);

        if (joinTreeNodePtr->right != NULL) {
            for (int i=0; i < depth; i++) fprintf(stderr,"    ");
            fprintf(stderr, "Right child has id = %d\n", joinTreeNodePtr->right->nodeId);
            if (joinTreeNodePtr->right->filterPtrs.size() > 0) {
                for (auto filter : joinTreeNodePtr->right->filterPtrs) {
                    for (int i=0; i < depth; i++) fprintf(stderr,"    ");
                    fprintf(stderr, "Right child has filters = ");
                    fprintf(stderr,"%d.%d %c %ld\n", filter->filterColumn.binding, filter->filterColumn.colId,
                        filter->comparison, filter->constant);
                }
            }
            else {
                for (int i=0; i < depth; i++) fprintf(stderr,"    ");
                fprintf(stderr, "Right child has no filter\n");
            }
        }
        else {
            for (int i=0; i < depth; i++) fprintf(stderr,"    ");
            fprintf(stderr, "Node has no right child so this is a filter join\n");
        }

        for (int i=0; i < depth; i++) fprintf(stderr,"    ");
        fprintf(stderr, "Left child has id = %d\n", joinTreeNodePtr->left->nodeId);
        if (joinTreeNodePtr->left->filterPtrs.size() > 0) {
            for (auto filter : joinTreeNodePtr->left->filterPtrs) {
                for (int i=0; i < depth; i++) fprintf(stderr,"    ");
                fprintf(stderr, "Left child has filters = ");
                fprintf(stderr,"%d.%d %c %ld\n", filter->filterColumn.binding, filter->filterColumn.colId,
                    filter->comparison, filter->constant);
            }
        }
        else {
            for (int i=0; i < depth; i++) fprintf(stderr,"    ");
            fprintf(stderr, "Left child has no filter\n");
        }

        joinTreeNodePtr = joinTreeNodePtr->left;
        depth++;
    }
    fprintf(stderr, "\n");
}

// Fills the columnInfo matrix with the data of every column
void QueryPlan::fillColumnInfo(Joiner& joiner) {
    Relation* relation;
    int relationsCount = joiner.getRelationsCount(); // Get the number of relations
    int columnsCount; // Number of columns of a relation

    // Allocate memory for every relation
    columnInfos = (ColumnInfo**) malloc(relationsCount * sizeof(ColumnInfo*));

    // For every relation get its column statistics
    for (int rel = 0; rel < relationsCount; rel++) {
        // Get the number of columns
        relation = &(joiner.getRelation(rel));
        columnsCount = relation->columns.size();

        //if (rel >= 24) fprintf(stderr, "relation %d\n", rel);

        // Allocate memory for the columns
        columnInfos[rel] = (ColumnInfo*) malloc(columnsCount * sizeof(ColumnInfo));

        // Get the info of every column
        for (int col = 0; col < columnsCount; col++) {
            uint64_t minimum = numeric_limits<uint64_t>::max();
            uint64_t maximum = 0;
            uint64_t tuples  = relation->size;
            uint64_t element;
            // One pass for the minimum and maximum
            for (int i = 0; i < tuples; i++) {
                element = relation->columns[col][i];
                if (element > maximum) maximum = element;
                if (element < minimum) minimum = element;
            }

            //if (rel >= 24) fprintf(stderr, "   column %d min = %-5d max = %-5d\n", col, minimum, maximum);

            // One pass for the distinct elements
            vector<bool> distinctElements(maximum - minimum + 1, false);
            uint64_t distinctCounter = 0;

            for (int i = 0; i < tuples; i++) {
                element = relation->columns[col][i];
                if (distinctElements[element - minimum] == false) {
                    distinctCounter++;
                    distinctElements[element - minimum] = true;
                }
            }

            // Save the infos
            columnInfos[rel][col].min      = minimum;
            columnInfos[rel][col].max      = maximum;
            columnInfos[rel][col].size     = tuples;
            columnInfos[rel][col].distinct = distinctCounter;
            columnInfos[rel][col].n        = maximum - minimum + 1;
            columnInfos[rel][col].spread   = (((double) (maximum - minimum + 1)) / ((double) (columnInfos[rel][col].distinct)));

            columnInfos[rel][col].counter = 0;
            columnInfos[rel][col].isSelectionColumn = false;
        }
    }
}
/*
// JoinTree destructor
void JoinTree::destrJoinTree() {
    // destruct query-tree in a DFS fassion
    JoinTreeNode *currNodePtr = this->root;
    bool from_left = false;

    while(currNodePtr) {
        // if there are left children
        if (currNodePtr->left) {
            currNodePtr = currNodePtr->left;
            from_left = true;
        }
        // if there are right children
        else if (currNodePtr->right) {
            currNodePtr = currNodePtr->right;
            from_left = false;
        }
        // if there are no left or right children
        else {
            // clean-up node
            free(currNodePtr->filterPtr);
            currNodePtr->filterPtr = NULL;
            free(currNodePtr->predicatePtr);
            currNodePtr->predicatePtr = NULL;
            // essentially, not head node
            if (currNodePtr->parent) {
                // go to the parent
                currNodePtr = currNodePtr->parent;
                // free the correct child's node
                if (from_left) {
                    free(currNodePtr->left);
                    currNodePtr->left = NULL;
                }
                else {
                    free(currNodePtr->right);
                    currNodePtr->right = NULL;
                }
            }
            // essentially, head node
            else {
                free(currNodePtr);
                currNodePtr = NULL;
            }
        }
    }
}

// QueryPlan destructor
void QueryPlan::destrQueryPlan(Joiner& joiner) {
    int relationsCount = joiner.getRelationsCount(); // Get the number of relations

    joinTreePtr->destrJoinTree();

    // For every relation get its column statistics
    for (int rel = 0; rel < relationsCount; rel++)
        // Allocate memory for the columns
        free(columnInfos[rel]);

    free(columnInfos);
}
*/
