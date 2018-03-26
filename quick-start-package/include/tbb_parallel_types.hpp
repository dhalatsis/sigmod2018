
#include "tbb/tbb.h"
#include "tbb/parallel_reduce.h"
#include "tbb/blocked_range.h"


using namespace tbb;

/* Ctruct for praralle check sum */
struct CheckSumT
{
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
