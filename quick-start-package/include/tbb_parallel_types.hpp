
#include "tbb/tbb.h"
#include "tbb/parallel_reduce.h"
#include "tbb/parallel_for.h"
#include "tbb/blocked_range.h"
#include "tuple_buffer.h"


using namespace tbb;

/* Struct for praralle check sum */
struct CheckSumT {

public:
    uint64_t my_sum;

    /* Initial constructor */
    CheckSumT(uint64_t * dataPtr, unsigned * row_ids, unsigned rnum, int idx)
    :col{dataPtr}, rids{row_ids}, rels_num{rnum}, tbi{idx}, my_sum(0)
    {}

    /* Slpitting constructor */
    CheckSumT(CheckSumT & x, split)
    :col{x.col}, rids{x.rids}, rels_num{x.rels_num}, tbi{x.tbi}, my_sum(0) {}

    /* How to join the thiefs */
    void join(const CheckSumT & y) {my_sum += y.my_sum;}

    /* The function call overloaded operator */
    void operator()(const tbb::blocked_range<size_t>& range) {
        uint64_t sum = my_sum;
        for (size_t i = range.begin(); i < range.end(); ++i)
            sum += col[rids[i*rels_num + tbi]];

        my_sum = sum;
    }

private:
    uint64_t * col;
    unsigned * rids;
    unsigned   rels_num;
    int  tbi;
};

/*----------- 4 Structs for parallelizing create table t ----------------*/
struct TableAllIntermediateCT {
public:

    /* Initial constructor */
    TableAllIntermediateCT
    (
        tuple_t  * tups,
        unsigned * rids_res, unsigned * rids_r, unsigned * rids_s,
        std::vector<unsigned> * help_v_r, std::vector<unsigned> * help_v_s,
        int start_idx, int old_relnum_r, int old_relnum_s, int num_relations
    )
    :tups{tups},
    rids_res{rids_res}, rids_r{rids_r}, rids_s{rids_s},
    help_v_r{help_v_r}, help_v_s{help_v_s},
    start_idx{start_idx}, old_relnum_r{old_relnum_r}, old_relnum_s{old_relnum_s}, num_relations{num_relations}
    {}

    /* The function call overloaded operator */
    void operator()(const tbb::blocked_range<size_t>& range) const {
        uint32_t row_i;
        for (size_t i = range.begin(); i < range.end(); ++i) {
            row_i = tups[i].key;
            for (size_t j = 0; j < (*help_v_r).size(); j++) {
                rids_res[(start_idx + i)*num_relations + j] =  rids_r[row_i*old_relnum_r + (*help_v_r)[j]];
            }

            row_i = tups[i].payload;
            for (size_t j = 0; j < (*help_v_s).size(); j++) {
                rids_res[(start_idx + i)*num_relations + j + (*help_v_r).size()] = rids_s[row_i*old_relnum_s + (*help_v_s)[j]];
            }
        }
    }

private:
    tuple_t  * tups;
    unsigned * rids_res;
    unsigned * rids_r;
    unsigned * rids_s;
    std::vector<unsigned> * help_v_r;
    std::vector<unsigned> * help_v_s;
    int start_idx;
    int old_relnum_r;
    int old_relnum_s;
    int num_relations;
};

struct TableRIntermediateCT {
public:

    /* Initial constructor */
    TableRIntermediateCT
    (
        tuple_t  * tups,
        unsigned * rids_res, unsigned * rids_r, unsigned * rids_s,
        std::vector<unsigned> * help_v_r, std::vector<unsigned> * help_v_s,
        int start_idx, int old_relnum_r, int old_relnum_s, int num_relations
    )
    :tups{tups},
    rids_res{rids_res}, rids_r{rids_r}, rids_s{rids_s},
    help_v_r{help_v_r}, help_v_s{help_v_s},
    start_idx{start_idx}, old_relnum_r{old_relnum_r}, old_relnum_s{old_relnum_s}, num_relations{num_relations}
    {}

    /* The function call overloaded operator */
    void operator()(const tbb::blocked_range<size_t>& range) const {
        uint32_t row_i;
        for (size_t i = range.begin(); i < range.end(); ++i) {
            row_i = tups[i].key;
            for (size_t j = 0; j < (*help_v_r).size(); j++) {
                rids_res[(start_idx + i)*num_relations + j] =  rids_r[row_i*old_relnum_r + (*help_v_r)[j]];
            }

            row_i = tups[i].payload;
            for (size_t j = 0; j < (*help_v_s).size(); j++) {
                rids_res[(start_idx + i)*num_relations + j + (*help_v_r).size()] = row_i;
            }
        }
    }

private:
    tuple_t  * tups;
    unsigned * rids_res;
    unsigned * rids_r;
    unsigned * rids_s;
    std::vector<unsigned> * help_v_r;
    std::vector<unsigned> * help_v_s;
    int start_idx;
    int old_relnum_r;
    int old_relnum_s;
    int num_relations;
};

struct TableSIntermediateCT {
public:

    /* Initial constructor */
    TableSIntermediateCT
    (
        tuple_t  * tups,
        unsigned * rids_res, unsigned * rids_r, unsigned * rids_s,
        std::vector<unsigned> * help_v_r, std::vector<unsigned> * help_v_s,
        int start_idx, int old_relnum_r, int old_relnum_s, int num_relations
    )
    :tups{tups},
    rids_res{rids_res}, rids_r{rids_r}, rids_s{rids_s},
    help_v_r{help_v_r}, help_v_s{help_v_s},
    start_idx{start_idx}, old_relnum_r{old_relnum_r}, old_relnum_s{old_relnum_s}, num_relations{num_relations}
    {}

    /* The function call overloaded operator */
    void operator()(const tbb::blocked_range<size_t>& range) const {
        uint32_t row_i;
        for (size_t i = range.begin(); i < range.end(); ++i) {
            row_i = tups[i].key;
            for (size_t j = 0; j < (*help_v_r).size(); j++) {
                rids_res[(start_idx + i)*num_relations + j] =  row_i;;
            }

            row_i = tups[i].payload;
            for (size_t j = 0; j < (*help_v_s).size(); j++) {
                rids_res[(start_idx + i)*num_relations + j + (*help_v_r).size()] = rids_s[row_i*old_relnum_s + (*help_v_s)[j]];
            }
        }
    }

private:
    tuple_t  * tups;
    unsigned * rids_res;
    unsigned * rids_r;
    unsigned * rids_s;
    std::vector<unsigned> * help_v_r;
    std::vector<unsigned> * help_v_s;
    int start_idx;
    int old_relnum_r;
    int old_relnum_s;
    int num_relations;
};

struct TableNoneIntermediateCT {
public:

    /* Initial constructor */
    TableNoneIntermediateCT
    (
        tuple_t  * tups,
        unsigned * rids_res, unsigned * rids_r, unsigned * rids_s,
        std::vector<unsigned> * help_v_r, std::vector<unsigned> * help_v_s,
        int start_idx, int old_relnum_r, int old_relnum_s, int num_relations
    )
    :tups{tups},
    rids_res{rids_res}, rids_r{rids_r}, rids_s{rids_s},
    help_v_r{help_v_r}, help_v_s{help_v_s},
    start_idx{start_idx}, old_relnum_r{old_relnum_r}, old_relnum_s{old_relnum_s}, num_relations{num_relations}
    {}

    /* The function call overloaded operator */
    void operator()(const tbb::blocked_range<size_t>& range) const {
        uint32_t row_i;
        for (size_t i = range.begin(); i < range.end(); ++i) {
            row_i = tups[i].key;
            for (size_t j = 0; j < (*help_v_r).size(); j++) {
                rids_res[(start_idx + i)*num_relations + j] =  row_i;
            }

            row_i = tups[i].payload;
            for (size_t j = 0; j < (*help_v_s).size(); j++) {
                rids_res[(start_idx + i)*num_relations + j + (*help_v_r).size()] = row_i;
            }
        }
    }

private:
    tuple_t  * tups;
    unsigned * rids_res;
    unsigned * rids_r;
    unsigned * rids_s;
    std::vector<unsigned> * help_v_r;
    std::vector<unsigned> * help_v_s;
    int start_idx;
    int old_relnum_r;
    int old_relnum_s;
    int num_relations;
};
/*-----------------------------------------------------------*/

/* Create Relation T parallel ctruct */
struct RelationNonIntermediateCT {
public:

    /* Initial constructor */
    RelationNonIntermediateCT ( tuple_t * tups, uint64_t * values )
    :tups{tups}, values{values}
    {}

    /* The function call overloaded operator */
    void operator()(const tbb::blocked_range<size_t>& range) const {

        /* Initialize the tuple array */
        for (size_t i = range.begin(); i < range.end(); ++i) {
            tups[i].key     = values[i];
            tups[i].payload = i;
        }
    }

private:
    tuple_t  * tups;
    uint64_t * values;
};

/* Create Relation T parallel ctruct */
struct RelationIntermediateCT {
public:

    /* Initial constructor */
    RelationIntermediateCT ( tuple_t * tups, uint64_t * values, unsigned * rids, unsigned num_relations, unsigned tbi )
    :tups{tups}, values{values}, rids{rids}, relations_num{num_relations}, table_index{tbi}
    {}

    /* The function call overloaded operator */
    void operator()(const tbb::blocked_range<size_t>& range) const {

        /* Initialize the tuple array */
        for (size_t i = range.begin(); i < range.end(); ++i) {
            tups[i].key     = values[rids[i*relations_num + table_index]];
            tups[i].payload = i;
        }
    }

private:
    tuple_t  * tups;
    uint64_t * values;
    unsigned * rids;
    unsigned   relations_num;
    unsigned   table_index;
};
