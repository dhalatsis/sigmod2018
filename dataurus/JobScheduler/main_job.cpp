#include "main_job.h"
#include "time.h"

static double timeExecute = 0;
static double timeTreegen = 0;

/*
int JobMain::Run(Joiner & joiner) {
    QueryPlan & queryPlan = queryPlan_;
    string    & line      = line_;
    QueryInfo i;

    double timeMain;
    struct timeval start;
    gettimeofday(&start, NULL);

    // Parse the query
    i.parseQuery(line);
    cleanQuery(i);

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

    //Print the result
    result_ = result_str;

    struct timeval end;
    gettimeofday(&end, NULL);
    timeMain = (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
    if (joiner.getRelationsCount() > 31)
        fprintf(stderr, "%d/%d:%ld-%lu\n", qn_, joiner.mst, (long) (timeMain * 1000), optimalJoinTree->root->treeCost);
}
*/

int JobMain::Run(Joiner & joiner) {

    // Count Query Time
    double timeMain;
    struct timeval start;
    gettimeofday(&start, NULL);

    // Execute the JoinTree
    bool stop = false;
    table_t* result = joinTreePtr_->root->execute(joinTreePtr_->root, joiner, *i_, result_, &stop, query_no_);

    // Count query time
    struct timeval end;
    gettimeofday(&end, NULL);
    timeMain = (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;

    // Free unused vars
    delete i_;
    delete joinTreePtr_;
}
