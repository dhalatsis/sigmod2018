#pragma once
#include <stdint.h>
#include "Joiner.hpp"
#include "job_scheduler.h"


int getRange(int threads, unsigned size);

// Args for allthefilters function
struct allfilters_arg {
    unsigned low;
    unsigned high;
    unsigned prefix;
    vector<uint64_t*> * columns;
    vector<FilterInfo*> * filterPtrs;
    unsigned * new_array;
};


// Args for Self Join
struct self_join_arg {
    unsigned low;
    unsigned high;
    uint64_t * column_values_l;
    uint64_t * column_values_r;
    unsigned * row_ids_matrix;
    unsigned * new_row_ids_matrix;
    unsigned rels_number;
    unsigned new_tbi;
    size_t i;
};

// Args for No Construct Self Join Find Indexes
struct no_constr_self_join_find_idx_arg {
    unsigned low;
    unsigned high;
    table_t * table;
    PredicateInfo * predicate_ptr;
    int index_l;
    int index_r;
};

// Args for No Construct Self Join add row_id to all the relations
struct no_constr_self_join_keep_rowids {
    unsigned low;
    unsigned high;
    uint64_t * column_values_l;
    uint64_t * column_values_r;
    std::vector<std::vector<uint64_t>> & row_ids_matrix;
    int index_l;
    int index_r;
    int relations_num;
    std::vector<SelectInfo> & selections;
    table_t * table;
    vector<uint64_t>  & checksums;
    Joiner * joinerPtr;
};

// Args for No Construct Self Join check sum printing
struct no_constr_self_join_checksum {
    unsigned low;
    unsigned high;
    vector<uint64_t>  & checksums;
    string local_result_str;
};

// Args for intermediate functions
struct inter_arg {
    unsigned low;
    unsigned high;
    unsigned prefix;
    uint64_t filter;
    uint64_t * values;
    unsigned * new_array;
    unsigned * old_rids;
    unsigned rel_num;
    unsigned table_index;
};

// Args for Non intermediate functions
struct noninter_arg {
    unsigned low;
    unsigned high;
    unsigned prefix;
    uint64_t filter;
    uint64_t * values;
    unsigned * new_array;
};

class JobJoin: public Job {
public:
  virtual int Run() = 0;
  JobJoin() {}
  ~JobJoin() {};
};

class JobSelfJoin: public JobJoin {
public:
    struct self_join_arg & args_;

  JobSelfJoin(struct self_join_arg & args)
  :args_(args)
  {}

  ~JobSelfJoin() {};

  int Run();
};

class JobNoConstrSelfJoinFindIdx: public JobJoin {
public:
    struct no_constr_self_join_find_idx_arg & args_;

  JobNoConstrSelfJoinFindIdx(struct no_constr_self_join_find_idx_arg & args)
  :args_(args)
  {}

  ~JobNoConstrSelfJoinFindIdx() {};

  int Run();
};

class JobNoConstrSelfJoinKeepRowIds: public JobJoin {
public:
    struct no_constr_self_join_keep_rowids & args_;

  JobNoConstrSelfJoinKeepRowIds(struct no_constr_self_join_keep_rowids & args)
  :args_(args)
  {}

  ~JobNoConstrSelfJoinKeepRowIds() {};

  int Run();
};

class JobNoConstrSelfJoinChecksum: public JobJoin {
public:
    struct no_constr_self_join_checksum & args_;

  JobNoConstrSelfJoinChecksum(struct no_constr_self_join_checksum & args)
  :args_(args)
  {}

  ~JobNoConstrSelfJoinChecksum() {};

  int Run();
};

class JobFilter: public Job {
public:
  virtual int Run() = 0;
  JobFilter() {}
  ~JobFilter() {};
};


// ALL filters class
class JobAllNonInterFindSize : public JobFilter {
public:
    struct allfilters_arg & args_;

    JobAllNonInterFindSize(struct allfilters_arg & args)
    :args_(args)
    {}

    ~JobAllNonInterFindSize() {}

    int Run();
};

class JobAllNonInterFilter : public JobFilter {
public:
    struct allfilters_arg & args_;

    JobAllNonInterFilter(struct allfilters_arg & args)
    :args_(args)
    {}

    ~JobAllNonInterFilter() {}

    int Run();
};


// Less filter classes
class JobLessInterFindSize : public JobFilter {
public:
    struct inter_arg & args_;

    JobLessInterFindSize(struct inter_arg & args)
    :args_(args)
    {}

    ~JobLessInterFindSize() {}

    int Run();
};

class JobLessInterFilter : public JobFilter {
public:
    struct inter_arg & args_;

    JobLessInterFilter(struct inter_arg & args)
    :args_(args)
    {}

    ~JobLessInterFilter() {}

    int Run();
};

// Greater Filter classes
class JobGreaterInterFindSize : public JobFilter {
public:
    struct inter_arg & args_;

    JobGreaterInterFindSize(struct inter_arg & args)
    :args_(args)
    {}

    ~JobGreaterInterFindSize() {}

    int Run();
};

class JobGreaterInterFilter : public JobFilter {
public:
    struct inter_arg & args_;

    JobGreaterInterFilter(struct inter_arg & args)
    :args_(args)
    {}

    ~JobGreaterInterFilter() {}

    int Run();
};

// Equal Filter classes
class JobEqualInterFindSize : public JobFilter {
public:
    struct inter_arg & args_;

    JobEqualInterFindSize(struct inter_arg & args)
    :args_(args)
    {}

    ~JobEqualInterFindSize() {}

    int Run();
};

class JobEqualInterFilter : public JobFilter {
public:
    struct inter_arg & args_;

    JobEqualInterFilter(struct inter_arg & args)
    :args_(args)
    {}

    ~JobEqualInterFilter() {}

    int Run();
};

/*++++++++++++++++++++++++++++++++++++*/
/* NON INTERMERIATE PRALLEL FUNCTIONS */
/*++++++++++++++++++++++++++++++++++++*/

// Less filter classes
class JobLessNonInterFindSize : public JobFilter {
public:
    struct noninter_arg & args_;

    JobLessNonInterFindSize(struct noninter_arg & args)
    :args_(args)
    {}

    ~JobLessNonInterFindSize() {}

    int Run();
};

class JobLessNonInterFilter : public JobFilter {
public:
    struct noninter_arg & args_;

    JobLessNonInterFilter(struct noninter_arg & args)
    :args_(args)
    {}

    ~JobLessNonInterFilter() {}

    int Run();
};

// Greater Filter classes
class JobGreaterNonInterFindSize : public JobFilter {
public:
    struct noninter_arg & args_;

    JobGreaterNonInterFindSize(struct noninter_arg & args)
    :args_(args)
    {}

    ~JobGreaterNonInterFindSize() {}

    int Run();
};

class JobGreaterNonInterFilter : public JobFilter {
public:
    struct noninter_arg & args_;

    JobGreaterNonInterFilter(struct noninter_arg & args)
    :args_(args)
    {}

    ~JobGreaterNonInterFilter() {}

    int Run();
};

// Equal Filter classes
class JobEqualNonInterFindSize : public JobFilter {
public:
    struct noninter_arg & args_;

    JobEqualNonInterFindSize(struct noninter_arg & args)
    :args_(args)
    {}

    ~JobEqualNonInterFindSize() {}

    int Run();
};

class JobEqualNonInterFilter : public JobFilter {
public:
    struct noninter_arg & args_;

    JobEqualNonInterFilter(struct noninter_arg & args)
    :args_(args)
    {}

    ~JobEqualNonInterFilter() {}

    int Run();
};
