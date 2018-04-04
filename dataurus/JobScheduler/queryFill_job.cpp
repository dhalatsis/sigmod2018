#include "queryFill_job.h"

int JobFillQueryPlan::Run(Joiner & joiner) {
    queryPlan_.fillColumnInfo(joiner);
}
