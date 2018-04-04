#pragma once
#include "job_master.h"
#include "QueryPlan.hpp"

using namespace std;

class JobFillQueryPlan: public JobMaster {
public:
    QueryPlan& queryPlan_;
    JobFillQueryPlan(QueryPlan & qp) : queryPlan_(qp) {}
    ~JobFillQueryPlan() {};
    int Run(Joiner & joiner);
};
