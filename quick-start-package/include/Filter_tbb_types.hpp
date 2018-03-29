#include "Joiner.hpp"
#include "tbb_parallel_types.hpp"

#include <vector>
#include "tbb/tbb.h"
#include "tbb/parallel_reduce.h"
#include "tbb/parallel_for.h"
#include "tbb/blocked_range.h"

// generally it's better to be avoided but let it be here...
using namespace tbb;
using namespace std;

unsigned new_tbi;
spin_mutex FilterMutex;

/*----------- Struct to parallelize filter  ----------------*/
struct ParalleNonItermediateSizeFindEqualFilterT {
    uint64_t size;

    /* Initial constructor */
    ParalleNonItermediateSizeFindEqualFilterT ( uint64_t * values, int filter )
    : values{values}, filter{filter}, size(0)
    {}

    /* Slpitting constructor */
    ParalleNonItermediateSizeFindEqualFilterT(ParalleNonItermediateSizeFindEqualFilterT & x, split)
    : values{x.values}, filter{x.filter}, size(0)
    {}

    /* The function call overloaded operator */
    void operator()(const tbb::blocked_range<size_t>& range) {
        /* Do the write */
        for (size_t i = range.begin(); i < range.end(); ++i) {
            if (values[i] == filter) {
                size++;
            }
        }
    }

    /* The function to call on thread join */
    void join( ParalleNonItermediateSizeFindEqualFilterT& y ) {size += y.size;}

private:
    uint64_t * values;
    int filter;
};
struct ParalleNonItermediateSizeFindLessFilterT {
    uint64_t size;

    /* Initial constructor */
    ParalleNonItermediateSizeFindLessFilterT ( uint64_t * values, int filter )
    : values{values}, filter{filter}, size(0)
    {}

    /* Slpitting constructor */
    ParalleNonItermediateSizeFindLessFilterT(ParalleNonItermediateSizeFindLessFilterT & x, split)
    : values{x.values}, filter{x.filter}, size(0)
    {}

    /* The function call overloaded operator */
    void operator()(const tbb::blocked_range<size_t>& range) {
        /* Do the write */
        for (size_t i = range.begin(); i < range.end(); ++i) {
            if (values[i] < filter) {
                size++;
            }
        }
    }

    /* The function to call on thread join */
    void join( ParalleNonItermediateSizeFindLessFilterT& y ) {size += y.size;}

private:
    uint64_t * values;
    int filter;
};
struct ParalleNonItermediateSizeFindGreaterFilterT {
    uint64_t size;

    /* Initial constructor */
    ParalleNonItermediateSizeFindGreaterFilterT ( uint64_t * values, int filter )
    : values{values}, filter{filter}, size(0)
    {}

    /* Slpitting constructor */
    ParalleNonItermediateSizeFindGreaterFilterT(ParalleNonItermediateSizeFindGreaterFilterT & x, split)
    : values{x.values}, filter{x.filter}, size(0)
    {}

    /* The function call overloaded operator */
    void operator()(const tbb::blocked_range<size_t>& range) {
        /* Do the write */
        for (size_t i = range.begin(); i < range.end(); ++i) {
            if (values[i] > filter) {
                size++;
            }
        }
    }

    /* The function to call on thread join */
    void join( ParalleNonItermediateSizeFindGreaterFilterT& y ) {size += y.size;}

private:
    uint64_t * values;
    int filter;
};




struct ParallelItermediateEqualFilterT {
    uint64_t * values;
    unsigned * old_rids;
    int filter;
    unsigned new_tbi;
    unsigned * rids;
    uint64_t size;
    unsigned rel_num;
    unsigned table_index;

    /* Initial constructor */
    ParallelItermediateEqualFilterT ( uint64_t * values, unsigned * old_rids, unsigned rel_num, unsigned table_index, int filter )
    : values{values}, old_rids{old_rids}, rids{NULL}, rel_num(rel_num), table_index(table_index), filter{filter}, new_tbi(0), size(0)
    {}

    /* Slpitting constructor */
    ParallelItermediateEqualFilterT(ParallelItermediateEqualFilterT & x, split)
    : values{x.values}, old_rids{x.old_rids}, rids{NULL}, rel_num(x.rel_num), table_index(x.table_index), filter{x.filter}, new_tbi(0), size(0)
    {}

    /* The function call overloaded operator */
    void operator()(const tbb::blocked_range<size_t>& range) {
        /* Do the write */
        for (size_t i = range.begin(); i < range.end(); ++i) {
            if (values[old_rids[i*rel_num + table_index]] == filter) {
                if (new_tbi == size) {
                    size += 1000;
                    rids = (unsigned*) realloc(rids, size * sizeof(unsigned));
                }
                rids[new_tbi] = old_rids[i*rel_num + table_index];
                new_tbi++;
            }
        //     if (values[old_rids[i]] == filter) {
        //         // lock.acquire(FilterMutex);
        //         if (new_tbi == size) {
        //             size += 1000;
        //             rids = (unsigned*) realloc(rids, size * sizeof(unsigned));
        //         }
        //         rids[new_tbi] = old_rids[i];
        //         new_tbi++;
        //         // lock.release();
        //     }
        }
    }

    /* The function to call on thread join */
    void join( ParallelItermediateEqualFilterT& rhs ) {
        if (rhs.new_tbi) {
            // unsigned * result = new unsigned[new_tbi + rhs.new_tbi];
            unsigned * result = (unsigned*) malloc((new_tbi + rhs.new_tbi) * sizeof(unsigned));
            copy(rids, rids + new_tbi, result);
            free (rids); // we copied them, we are done with them
            copy(rhs.rids, rhs.rids + rhs.new_tbi, result + new_tbi);
            size += rhs.size;
            rids = result;
            new_tbi += rhs.new_tbi;
        }
    }
};

/* Create Relation T parallel ctruct */
struct ParallelNonItermediateEqualFilterT {
    uint64_t * values;
    unsigned * old_rids;
    unsigned * rids;
    unsigned * result;
    unsigned new_tbi;
    int filter;
    uint64_t size;

    /* Initial constructor */
    ParallelNonItermediateEqualFilterT ( uint64_t * values, unsigned * old_rids, int filter, unsigned * result )
    : values{values}, old_rids{old_rids}, rids{NULL}, filter{filter}, result{result}, new_tbi(0), size(0)
    {}

    /* Slpitting constructor */
    ParallelNonItermediateEqualFilterT(ParallelNonItermediateEqualFilterT & x, split)
    : values{x.values}, old_rids{x.old_rids}, rids{NULL}, filter{x.filter}, result{x.result}, new_tbi(0), size(0)
    {}

    /* The function call overloaded operator */
    void operator()(const tbb::blocked_range<size_t>& range) {
        /* Do the write */
        for (size_t i = range.begin(); i < range.end(); ++i) {
            if (values[i] == filter) {
                if (new_tbi == size) {
                    size += 1000;
                    //std::cerr << "In realloc " << i << '\n';
                    rids = (unsigned*) realloc(rids, size * sizeof(unsigned));
                }
                rids[new_tbi] = i;
                new_tbi++;
            }
        }
    }

    /* The function to call on thread join */
    void join( ParallelNonItermediateEqualFilterT& rhs ) {
        if (rhs.new_tbi) {
            //std::cerr << "In JOIN " <<  new_tbi << " " <<  rhs.new_tbi << '\n';
            // unsigned * result = new unsigned[new_tbi + rhs.new_tbi];
            unsigned * result = (unsigned*) malloc((new_tbi + rhs.new_tbi) * sizeof(unsigned)); //Added results here
            copy(rids, rids + new_tbi, result);
            free (rids); // we copied them, we are done with them
            copy(rhs.rids, rhs.rids + rhs.new_tbi, result + new_tbi);
            size += rhs.size;
            rids = result;
            new_tbi += rhs.new_tbi;
        }
    }
};

/* Create Relation T parallel ctruct */
struct ParallelItermediateGreaterFilterT {
    uint64_t * values;
    unsigned * old_rids;
    int filter;
    unsigned new_tbi;
    unsigned * rids;
    uint64_t size;
    unsigned rel_num;
    unsigned table_index;

    /* Initial constructor */
    ParallelItermediateGreaterFilterT ( uint64_t * values, unsigned * old_rids, unsigned rel_num, unsigned table_index, int filter )
    : values{values}, old_rids{old_rids}, rids{NULL}, rel_num(rel_num), table_index(table_index), filter{filter}, new_tbi(0), size(0)
    {}

    /* Slpitting constructor */
    ParallelItermediateGreaterFilterT(ParallelItermediateGreaterFilterT & x, split)
    : values{x.values}, old_rids{x.old_rids}, rids{NULL}, rel_num(x.rel_num), table_index(x.table_index), filter{x.filter}, new_tbi(0), size(0)
    {}

    /* The function call overloaded operator */
    void operator()(const tbb::blocked_range<size_t>& range) {
        /* Do the write */
        for (size_t i = range.begin(); i < range.end(); ++i) {
            if (values[old_rids[i*rel_num + table_index]] > filter) {
                if (new_tbi == size) {
                    size += 1000;
                    rids = (unsigned*) realloc(rids, size * sizeof(unsigned));
                }
                rids[new_tbi] = old_rids[i*rel_num + table_index];
                new_tbi++;
            }
            // if (values[old_rids[i]] > filter) {
            //     // lock.acquire(FilterMutex);
            //     if (new_tbi == size) {
            //         size += 1000;
            //         rids = (unsigned*) realloc(rids, size * sizeof(unsigned));
            //     }
            //     rids[new_tbi] = old_rids[i];
            //     new_tbi++;
            //     // lock.release();
            // }
        }
    }

    /* The function to call on thread join */
    void join( ParallelItermediateGreaterFilterT& rhs ) {
        if (rhs.new_tbi) {
            // unsigned * result = new unsigned[new_tbi + rhs.new_tbi];
            unsigned * result = (unsigned*) malloc((new_tbi + rhs.new_tbi) * sizeof(unsigned));
            copy(rids, rids + new_tbi, result);
            free (rids); // we copied them, we are done with them
            copy(rhs.rids, rhs.rids + rhs.new_tbi, result + new_tbi);
            size += rhs.size;
            rids = result;
            new_tbi += rhs.new_tbi;
        }
    }
};

/* Create Relation T parallel ctruct */
struct ParallelNonItermediateGreaterFilterT {
    uint64_t * values;
    unsigned * old_rids;
    int filter;
    unsigned new_tbi;
    unsigned * rids;
    uint64_t size;

    /* Initial constructor */
    ParallelNonItermediateGreaterFilterT ( uint64_t * values, unsigned * old_rids, int filter )
    : values{values}, old_rids{old_rids}, rids{NULL}, filter{filter}, new_tbi(0), size(0)
    {}

    /* Slpitting constructor */
    ParallelNonItermediateGreaterFilterT(ParallelNonItermediateGreaterFilterT & x, split)
    : values{x.values}, old_rids{x.old_rids}, rids{NULL}, filter{x.filter}, new_tbi(0), size(0)
    {}

    /* The function call overloaded operator */
    void operator()(const tbb::blocked_range<size_t>& range) {
        /* Do the write */
        for (size_t i = range.begin(); i < range.end(); ++i) {
            if (values[i] > filter) {
                if (new_tbi == size) {
                    size += 1000;
                    rids = (unsigned*) realloc(rids, size * sizeof(unsigned));
                }
                rids[new_tbi] = i;
                new_tbi++;
            }
        }
    }

    /* The function to call on thread join */
    void join( ParallelNonItermediateGreaterFilterT& rhs ) {
        if (rhs.new_tbi) {
            // unsigned * result = new unsigned[new_tbi + rhs.new_tbi];
            unsigned * result = (unsigned*) malloc((new_tbi + rhs.new_tbi) * sizeof(unsigned));
            copy(rids, rids + new_tbi, result);
            free (rids); // we copied them, we are done with them
            copy(rhs.rids, rhs.rids + rhs.new_tbi, result + new_tbi);
            size += rhs.size;
            rids = result;
            new_tbi += rhs.new_tbi;
        }
    }
};

/* Create Relation T parallel ctruct */
struct ParallelItermediateLessFilterT {
    uint64_t * values;
    unsigned * old_rids;
    int filter;
    unsigned new_tbi;
    unsigned * rids;
    uint64_t size;
    unsigned rel_num;
    unsigned table_index;

    /* Initial constructor */
    ParallelItermediateLessFilterT ( uint64_t * values, unsigned * old_rids, unsigned rel_num, unsigned table_index, int filter )
    : values{values}, old_rids{old_rids}, rids{NULL}, rel_num(rel_num), table_index(table_index), filter{filter}, new_tbi(0), size(0)
    {}

    /* Slpitting constructor */
    ParallelItermediateLessFilterT(ParallelItermediateLessFilterT & x, split)
    : values{x.values}, old_rids{x.old_rids}, rids{NULL}, rel_num(x.rel_num), table_index(x.table_index), filter{x.filter}, new_tbi(0), size(0)
    {}

    /* The function call overloaded operator */
    void operator()(const tbb::blocked_range<size_t>& range) {
        /* Do the write */
        for (size_t i = range.begin(); i < range.end(); ++i) {
            if (values[old_rids[i*rel_num + table_index]] < filter) {
                if (new_tbi == size) {
                    size += 1000;
                    rids = (unsigned*) realloc(rids, size * sizeof(unsigned));
                }
                rids[new_tbi] = old_rids[i*rel_num + table_index];
                new_tbi++;
            }
            // if (values[old_rids[i]] < filter) {
            //     // lock.acquire(FilterMutex);
            //     if (new_tbi == size) {
            //         size += 1000;
            //         rids = (unsigned*) realloc(rids, size * sizeof(unsigned));
            //     }
            //     rids[new_tbi] = old_rids[i];
            //     new_tbi++;
            //     // lock.release();
            // }
        }
    }

    /* The function to call on thread join */
    void join( ParallelItermediateLessFilterT& rhs ) {
        if (rhs.new_tbi) {
            // unsigned * result = new unsigned[new_tbi + rhs.new_tbi];
            unsigned * result = (unsigned*) malloc((new_tbi + rhs.new_tbi) * sizeof(unsigned));
            copy(rids, rids + new_tbi, result);
            free (rids); // we copied them, we are done with them
            copy(rhs.rids, rhs.rids + rhs.new_tbi, result + new_tbi);
            size += rhs.size;
            rids = result;
            new_tbi += rhs.new_tbi;
        }
    }
};

/* Create Relation T parallel ctruct */
struct ParallelNonItermediateLessFilterT {
    uint64_t * values;
    unsigned * old_rids;
    int filter;
    unsigned new_tbi;
    unsigned * rids;
    uint64_t size;

    /* Initial constructor */
    ParallelNonItermediateLessFilterT ( uint64_t * values, unsigned * old_rids, int filter )
    : values{values}, old_rids{old_rids}, rids{NULL}, filter{filter}, new_tbi(0), size(0)
    {}

    /* Slpitting constructor */
    ParallelNonItermediateLessFilterT(ParallelNonItermediateLessFilterT & x, split)
    : values{x.values}, old_rids{x.old_rids}, rids{NULL}, filter{x.filter}, new_tbi(0), size(0)
    {}

    /* The function call overloaded operator */
    void operator()(const tbb::blocked_range<size_t>& range) {
        /* Do the write */
        for (size_t i = range.begin(); i < range.end(); ++i) {
            if (values[i] < filter) {
                if (new_tbi == size) {
                    size += 1000;
                    rids = (unsigned*) realloc(rids, size * sizeof(unsigned));
                }
                rids[new_tbi] = i;
                new_tbi++;
            }
        }
    }

    /* The function to call on thread join */
    void join( ParallelNonItermediateLessFilterT& rhs ) {
        if (rhs.new_tbi) {
            // unsigned * result = new unsigned[new_tbi + rhs.new_tbi];
            unsigned * result = (unsigned*) malloc((new_tbi + rhs.new_tbi) * sizeof(unsigned));
            copy(rids, rids + new_tbi, result);
            free (rids); // we copied them, we are done with them
            copy(rhs.rids, rhs.rids + rhs.new_tbi, result + new_tbi);
            size += rhs.size;
            rids = result;
            new_tbi += rhs.new_tbi;
        }
    }
};

// TODO
/* Create Relation T parallel ctruct */
struct ParallelSelfJoinT {
    unsigned * row_ids_matrix;
    unsigned * new_row_ids_matrix;
    uint64_t *column_values_l;
    uint64_t *column_values_r;
    int index_l;
    int index_r;
    unsigned rows_number;
    unsigned rels_number;
    unsigned new_tbi;
    uint64_t size;

    /* Initial constructor */
    ParallelSelfJoinT ( unsigned* row_ids_matrix, uint64_t* column_values_l, uint64_t* column_values_r, int index_l, int index_r, unsigned rows_number, unsigned rels_number )
    : row_ids_matrix(row_ids_matrix), new_row_ids_matrix(NULL), column_values_l(column_values_l), column_values_r(column_values_r), index_l(index_l), index_r(index_r), rows_number(rows_number), rels_number(rels_number), new_tbi(0), size(0)
    {}

    /* Slpitting constructor */
    ParallelSelfJoinT(ParallelSelfJoinT & x, split)
    : row_ids_matrix(x.row_ids_matrix), new_row_ids_matrix(NULL), column_values_l(x.column_values_l), column_values_r(x.column_values_r), index_l(x.index_l), index_r(x.index_r), rows_number(x.rows_number), rels_number(x.rels_number), new_tbi(0), size(0)
    {}

    /* The function call overloaded operator */
    void operator()(const tbb::blocked_range<size_t>& range) {
        for (size_t i = range.begin(); i < range.end(); ++i) {
            // /* Apply the predicate: In case of success add to new table */
            // if (column_values_l[row_ids_matrix[i*rels_number + index_l]] == column_values_r[row_ids_matrix[i*rels_number + index_r]]) {
            //     /* Add this row_id to all the relations */
            //     for (ssize_t relation = 0; relation < rels_number; relation++) {
            //         new_row_ids_matrix[new_tbi*rels_number  + relation] = row_ids_matrix[i*rels_number  + relation];
            //     }
            //     new_tbi++;
            // }
        }
    }

    /* The function to call on thread join */
    void join( ParallelSelfJoinT& rhs ) {
        if (rhs.new_tbi) {
            // unsigned * result = new unsigned[new_tbi + rhs.new_tbi];
            unsigned * result = (unsigned*) malloc((new_tbi + rhs.new_tbi) * sizeof(unsigned));
            copy(new_row_ids_matrix, new_row_ids_matrix + new_tbi, result);
            free (new_row_ids_matrix); // we copied them, we are done with them
            copy(rhs.new_row_ids_matrix, rhs.new_row_ids_matrix + rhs.new_tbi, result + new_tbi);
            size += rhs.size;
            new_row_ids_matrix = result;
            new_tbi += rhs.new_tbi;
        }
    }
};
