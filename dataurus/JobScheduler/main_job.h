#pragma once
#include "job_master.h"
#include "Joiner.hpp"

using namespace std;

/*
class JobMain: public JobMaster {
public:
    QueryPlan& queryPlan_;
    string line_;
    string result_;
    int qn_;

    JobMain(QueryPlan & qp, string & line, int qn) : queryPlan_(qp), line_(line), qn_(qn) {}
    ~JobMain() {};
    int Run(Joiner & joiner);
};
*/

class JobMain: public JobMaster {
public:
    QueryInfo * i_;
    JoinTree  * joinTreePtr_;
    string result_;
    int query_no_;

    JobMain(QueryInfo * i, string & line, JoinTree * joinTreePtr, int query_no) : i_(i), joinTreePtr_(joinTreePtr), query_no_(query_no) {}
    ~JobMain() {};
    int Run(Joiner & joiner);
};
