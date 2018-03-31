#include "job_scheduler.h"
#include "types.h"


// Args for Non intermediate functions
struct noninterRel_arg {
    unsigned low;
    unsigned high;
    tuple_t  * tups;
    uint64_t * values;
};

// Args for intermediate functions
struct interRel_arg {
    unsigned low;
    unsigned high;
    uint64_t * values;
    tuple_t  * tups;
    unsigned * rids;
    unsigned rel_num;
    unsigned table_index;
};

class CreateJob : public Job {
public:
    CreateJob() {}

    ~CreateJob() {};

    virtual int Run()=0;
};


class JobCreateNonInterRel : public CreateJob {
public:
    struct noninterRel_arg & args_;

    JobCreateNonInterRel(struct noninterRel_arg & args)
    :args_(args)
    {}

    ~JobCreateNonInterRel() {};

    int Run();
};

class JobCreateInterRel : public CreateJob {
public:
    struct interRel_arg & args_;

    JobCreateInterRel(struct interRel_arg & args)
    :args_(args)
    {}

    ~JobCreateInterRel() {};

    int Run();
};
