#include "main_job.h"

static double timeExecute = 0;
static double timeTreegen = 0;

int JobMain::Run(Joiner & joiner) {
    QueryPlan & queryPlan = queryPlan_;
    string    & line      = line_;
    QueryInfo i;

    // Parse the query
    //std::cerr << "Query:" << line.c_str() << '\n';
    i.parseQuery(line);
    cleanQuery(i);
    //q_counter++;

    #ifdef time
    struct timeval start, end;
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

    //Print the result
    //std::cout << result_str << endl;
    //write to result
    result_ = result_str;
    //result_ = "NOT IMPL";

    // #ifdef time
    // gettimeofday(&end, NULL);
    // timeExecute += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
    // #endif
}
