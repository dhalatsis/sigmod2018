#include "Joiner.hpp"
#include "tbb_parallel_types.hpp"

#include <vector>
#include <string.h>
#include "tbb/tbb.h"
#include "tbb/parallel_reduce.h"
#include "tbb/parallel_for.h"
#include "tbb/blocked_range.h"
#include "tbb/concurrent_vector.h"

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
            size = new_tbi + rhs.new_tbi;
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
    unsigned new_tbi;
    int filter;
    uint64_t size;

    /* Initial constructor */
    ParallelNonItermediateEqualFilterT ( uint64_t * values, unsigned * old_rids, int filter)
    : values{values}, old_rids{old_rids}, rids{NULL}, filter{filter}, new_tbi(0), size(0)
    {}

    /* Slpitting constructor */
    ParallelNonItermediateEqualFilterT(ParallelNonItermediateEqualFilterT & x, split)
    : values{x.values}, old_rids{x.old_rids}, rids{NULL}, filter{x.filter}, new_tbi(0), size(0)
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
            // unsigned * result = new unsigned[new_tbi + rhs.new_tbi];
            unsigned * result = (unsigned*) malloc((new_tbi + rhs.new_tbi) * sizeof(unsigned)); //Added results here
            copy(rids, rids + new_tbi, result);
            free (rids); // we copied them, we are done with them
            copy(rhs.rids, rhs.rids + rhs.new_tbi, result + new_tbi);
            size = new_tbi + rhs.new_tbi;
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
            size = new_tbi + rhs.new_tbi;
            rids = result;
            new_tbi += rhs.new_tbi;
        }
    }
};

/* Create Relation T parallel ctruct */
struct ParallelNonItermediateGreaterFilterT {
    uint64_t * values;
    int filter;
    unsigned new_tbi;
    unsigned * rids;
    uint64_t size;

    /* Initial constructor */
    ParallelNonItermediateGreaterFilterT ( uint64_t * values, int filter )
    : values{values}, rids{NULL}, filter{filter}, new_tbi(0), size(0)
    {}

    /* Slpitting constructor */
    ParallelNonItermediateGreaterFilterT(ParallelNonItermediateGreaterFilterT & x, split)
    : values{x.values}, rids{NULL}, filter{x.filter}, new_tbi(0), size(0)
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
            size = new_tbi + rhs.new_tbi;
            rids = result;
            new_tbi += rhs.new_tbi;
        }
    }
};

/* Create Relation T parallel ctruct */
struct ParallelBMT {
    uint64_t * values;
    int filter;
    int size;
    vector<bool> & bitmap;

    /* Initial constructor */
    ParallelBMT ( uint64_t * values, int filter , vector<bool> & bitmap)
    : values{values}, bitmap{bitmap}, filter{filter}, size(0)
    {}

    ParallelBMT (const ParallelBMT & x, split )
    : values{x.values}, bitmap{x.bitmap}, filter{filter}, size(0)
    {}

    void join (const ParallelBMT & y) {size += y.size;}

    /* The function call overloaded operator */
    void operator()(const tbb::blocked_range<size_t>& range) {
        /* Do the write */
        for (size_t i = range.begin(); i < range.end(); ++i) {
            if (values[i] > filter) {
                bitmap[i] = true;
                size++;
            }
        }
    }
};

// struct ParallelWriteT {
//     std::vector<bool> & bitmap;
//     unsigned * rids;
//     unsigned chunk_size;
//     unsigned mod;
//
//     /* Initial constructor */
//     ParallelWriteT ( unsigned * rids, std::vector<bool> & bitmap, unsigned cs, unsigned mod)
//     : rids{rids}, bitmap{bitmap}, chunk_size{cs}, mod{mod};
//     {}
//
//     /* The function call overloaded operator */
//     void operator()(const tbb::blocked_range<size_t>& range) const{
//
//         /* Do the write */
//         size_t i range.begin();
//         size_t bitmap_i = (i == 0) ? 0 : i * chunk_size + mod;
//         size_t size     = (i == 0) ? range.size() * chunk_size : range.size()  // range.end - range.start
//         for (size_t i = range.begin(); i < range.end(); ++i) {
//             rids[i] = vector[i];
//         }
//     }
// };

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
            size = new_tbi + rhs.new_tbi;
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
            size = new_tbi + rhs.new_tbi;
            rids = result;
            new_tbi += rhs.new_tbi;
        }
    }
};

// TODO
/* Create Relation T parallel utility struct */
struct ParallelSelfJoinUtilityT {
    unsigned * row_ids_matrix;
    unsigned * new_row_ids_matrix;
    unsigned * new_matrix;
    unsigned rels_number;
    uint64_t i;
    unsigned size;
    unsigned real_size;
    size_t range_beg;

    /* Initial constructor */
    ParallelSelfJoinUtilityT ( unsigned* row_ids_matrix, unsigned * new_row_ids_matrix, unsigned rels_number, uint64_t i )
    : row_ids_matrix(row_ids_matrix), new_row_ids_matrix(new_row_ids_matrix), new_matrix(NULL), rels_number(rels_number), i(i), size(0), real_size(0), range_beg(0)
    {}

    /* Splitting constructor */
    ParallelSelfJoinUtilityT(ParallelSelfJoinUtilityT & x, split)
    : row_ids_matrix(x.row_ids_matrix), new_row_ids_matrix(x.new_row_ids_matrix), new_matrix(NULL), rels_number(x.rels_number), i(x.i), size(0), real_size(0), range_beg(0)
    {}

    /* The function call overloaded operator */
    void operator()(const tbb::blocked_range<size_t>& range) {
        range_beg = range.begin();
        for (size_t relation = range.begin(); relation < range.end(); ++relation) {
            if (real_size == size) {
                size += 100;
                new_matrix = (unsigned*) realloc(new_matrix, size * sizeof(unsigned));
            }
            new_matrix[real_size] = row_ids_matrix[i*rels_number + relation];
            real_size++;
        }
    }

    /* The function to call on thread join */
    void join( ParallelSelfJoinUtilityT& rhs ) {
        if (rhs.real_size) {
            // unsigned * result = new unsigned[new_tbi + rhs.new_tbi];
            unsigned * result = (unsigned*) malloc((real_size + rhs.real_size) * sizeof(unsigned));
            copy(new_matrix, new_matrix + real_size, result);
            free (new_matrix); // we copied them, we are done with them
            copy(rhs.new_matrix, rhs.new_matrix + rhs.real_size, result + real_size);
            size = real_size + rhs.real_size;
            new_matrix = result;
            real_size += rhs.real_size;
            // memcpy(new_row_ids_matrix + new_tbi*rels_number + range_beg, new_matrix, real_size * sizeof(unsigned));
        }
    }
};

// // TODO
// /* Create Relation T parallel struct */
// struct ParallelSelfJoinT {
//     unsigned * row_ids_matrix;
//     unsigned * new_row_ids_matrix;
//     uint64_t *column_values_l;
//     uint64_t *column_values_r;
//     int index_l;
//     int index_r;
//     unsigned rels_number;
//     unsigned new_tbi;
//     ParallelSelfJoinUtilityT *psjut;
//     unsigned *interm;
//     uint64_t interm_size;
//
//     /* Initial constructor */
//     ParallelSelfJoinT ( unsigned* row_ids_matrix, unsigned * new_row_ids_matrix, uint64_t* column_values_l, uint64_t* column_values_r, int index_l, int index_r, unsigned rels_number )
//     : row_ids_matrix(row_ids_matrix), new_row_ids_matrix(new_row_ids_matrix), column_values_l(column_values_l), column_values_r(column_values_r), index_l(index_l), index_r(index_r), rels_number(rels_number), new_tbi(0), interm(NULL), interm_size(0)
//     {}
//
//     /* Splitting constructor */
//     ParallelSelfJoinT(ParallelSelfJoinT & x, split)
//     : row_ids_matrix(x.row_ids_matrix), new_row_ids_matrix(x.new_row_ids_matrix), column_values_l(x.column_values_l), column_values_r(x.column_values_r), index_l(x.index_l), index_r(x.index_r), rels_number(x.rels_number), new_tbi(0), interm(NULL), interm_size(0)
//     {}
//
//     /* The function call overloaded operator */
//     void operator()(const tbb::blocked_range<size_t>& range) {
//         for (size_t i = range.begin(); i < range.end(); ++i) {
//             /* Apply the predicate: In case of success add to new table */
//             if (column_values_l[row_ids_matrix[i*rels_number + index_l]] == column_values_r[row_ids_matrix[i*rels_number + index_r]]) {
//                 /* Add this row_id to all the relations */
//                 // for (ssize_t relation = 0; relation < rels_number; relation++) {
//                 //     new_row_ids_matrix[new_tbi*rels_number + relation] = row_ids_matrix[i*rels_number + relation];
//                 // }
//                 // ParallelSelfJoinUtilityT psjut( row_ids_matrix, rels_number, i );
//                 psjut = new ParallelSelfJoinUtilityT( row_ids_matrix, new_row_ids_matrix, rels_number, i );
//                 parallel_reduce(blocked_range<size_t>(0,rels_number,GRAINSIZE), *psjut);
//
//                 // memcpy(new_row_ids_matrix + new_tbi*rels_number, psjut->new_matrix, rels_number * sizeof(unsigned));
//                 interm = psjut->new_matrix;
//                 interm_size = psjut->real_size;
//
//                 new_tbi++;
//             }
//         }
//     }
//
//     /* The function to call on thread join */
//     void join( ParallelSelfJoinT& rhs ) {
//         if (rhs.interm_size) {
//             unsigned * result = (unsigned*) malloc((interm_size + rhs.interm_size) * sizeof(unsigned));
//             copy(new_matrix, new_matrix + real_size, result);
//             free (new_matrix); // we copied them, we are done with them
//             copy(rhs.new_matrix, rhs.new_matrix + rhs.real_size, result + real_size);
//             size = real_size + rhs.real_size;
//             new_matrix = result;
//             real_size += rhs.real_size;
//             // memcpy(new_row_ids_matrix + rhs.new_tbi*rels_number, psjut->new_matrix, psjut->real_size * sizeof(unsigned));
//             new_tbi += rhs.new_tbi;
//         }
//     }
// };
