#include <vector>

#include "job_scheduler.h"
#include "tuple_buffer.h"
#include "tbb_ontheflyChecksums_types.hpp"
#include "types.h"

using namespace std;



// Args for Non intermediate functions

// Args for intermediate functions
struct interInterSum_arg {
    vector<struct checksumST> * distinctPairs_r;
    vector<struct checksumST> * distinctPairs_s;
    chainedtuplebuffer_t * cb;
    uint64_t * priv_checsums;
    unsigned   rel_num_r;
    unsigned   rel_num_s;
    unsigned * rids_r;
    unsigned * rids_s;
};

// R inter S non Inter
struct interNoninterSum_arg {
    vector<struct checksumST> * distinctPairs_r;
    vector<struct checksumST> * distinctPairs_s;
    chainedtuplebuffer_t * cb;
    uint64_t * priv_checsums;
    unsigned   rel_num_r;
    unsigned * rids_r;
};

// R non inter S inter
struct noninterInterSum_arg {
    vector<struct checksumST> * distinctPairs_r;
    vector<struct checksumST> * distinctPairs_s;
    chainedtuplebuffer_t * cb;
    uint64_t * priv_checsums;
    unsigned   rel_num_s;
    unsigned * rids_s;
};

// R non inter S inter
struct noninterNoninterSum_arg {
    vector<struct checksumST> * distinctPairs_r;
    vector<struct checksumST> * distinctPairs_s;
    chainedtuplebuffer_t * cb;
    uint64_t * priv_checsums;
};

class JobChechkSum : public Job {
public:
    JobChechkSum() {}

    ~JobChechkSum() {};

    virtual int Run()=0;
};


class JobCheckSumInterInter : public JobChechkSum {
public:
    struct interInterSum_arg & args_;

    JobCheckSumInterInter(struct interInterSum_arg & args)
    :args_(args)
    {}

    ~JobCheckSumInterInter() {};

    int Run();
};

class JobCheckSumInterNonInter : public JobChechkSum {
public:
    struct interNoninterSum_arg & args_;

    JobCheckSumInterNonInter(struct interNoninterSum_arg & args)
    :args_(args)
    {}

    ~JobCheckSumInterNonInter() {};

    int Run();
};

// R Non intermediate S intermediate
class JobCheckSumNonInterInter : public JobChechkSum {
public:
    struct noninterInterSum_arg & args_;

    JobCheckSumNonInterInter(struct noninterInterSum_arg & args)
    :args_(args)
    {}

    ~JobCheckSumNonInterInter() {};

    int Run();
};

// R Non intermediate S Non intermediate
class JobCheckSumNonInterNonInter : public JobChechkSum {
public:
    struct noninterNoninterSum_arg & args_;

    JobCheckSumNonInterNonInter(struct noninterNoninterSum_arg & args)
    :args_(args)
    {}

    ~JobCheckSumNonInterNonInter() {};

    int Run();
};
