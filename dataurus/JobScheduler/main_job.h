#pragma once
#include "job_master.h"
#include "Joiner.hpp"

using namespace std;

class JobMain: public JobMaster {
public:
    QueryPlan& queryPlan_;
    string line_;
    string result_;

    JobMain(QueryPlan & qp, string & line) : queryPlan_(qp), line_(line) {}
    ~JobMain() {};
    int Run(Joiner & joiner);
};
