#include "job_scheduler.h"
#include "types.h"


// Args for Non intermediate functions
struct noninterSum_arg {
    unsigned low;
    unsigned high;
    tuple_t  * tups;
    uint64_t * values;
};

// Args for intermediate functions
struct interSum_arg {
    unsigned low;
    unsigned high;
    uint64_t * values;
    tuple_t  * tups;
    unsigned * rids;
    unsigned rel_num;
    unsigned table_index;
};

class JobChechkSum : public Job {
public:
    JobChechkSum() {}

    ~JobChechkSum() {};

    virtual int Run()=0;
};


class JobCheckSumNonInter : public JobChechkSum {
public:
    struct noninterSum_arg & args_;

    JobCheckSumNonInter(struct noninterSum_arg & args)
    :args_(args)
    {}

    ~JobCheckSumNonInter() {};

    int Run();
};

class JobCreateInterRel : public JobChechkSum {
public:
    struct interSum_arg & args_;

    JobCreateInterRel(struct interSum_arg & args)
    :args_(args)
    {}

    ~JobCreateInterRel() {};

    int Run();
};
