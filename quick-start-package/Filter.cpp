#include "Joiner.hpp"

#include "tbb/tbb.h"
#include "tbb/parallel_reduce.h"
#include "tbb/parallel_for.h"
#include "tbb/blocked_range.h"


using namespace tbb;
using namespace std;
spin_mutex FilterMutex;


/*----------- Struct to parallelize filter  ----------------*/
/* Create Relation T parallel ctruct */
struct ParallelNonItermediateFilterT {

public:
    unsigned new_tbi;

    /* Initial constructor */
    ParallelNonItermediateFilterT ( uint64_t * values, unsigned * old_rids, unsigned * rids, int filter )
    :values{values}, old_rids{old_rids}, rids{rids}, filter{filter}, new_tbi(0)
    {}

    /* The function call overloaded operator */
    void operator()(const tbb::blocked_range<size_t>& range) const {

        /* Do the write */
        spin_mutex::scoped_lock lock;
        for (size_t i = range.begin(); i < range.end(); ++i) {
            if (values[i] == filter) {
                lock.acquire(FilterMutex);
                rids[new_tbi] = i;
                //new_tbi++;
                lock.release();
            }
        }
    }

private:
    uint64_t * values;
    unsigned * old_rids;
    unsigned * rids;
    int filter;
};


double timeSelfJoin = 0;
double timeSelectFilter = 0;


/* The self Join Function */
table_t * Joiner::SelfJoin(table_t *table, PredicateInfo *predicate_ptr, columnInfoMap & cmap) {

#ifdef time
    struct timeval start;
    gettimeofday(&start, NULL);
#endif

    /* Create - Initialize a new table */
    table_t *new_table            = new table_t;
    new_table->relations_bindings = std::unordered_map<unsigned, unsigned>(table->relations_bindings);
    new_table->intermediate_res   = true;
    new_table->column_j           = new column_t;
    new_table->rels_num           = table->rels_num;
    new_table->row_ids  = (unsigned *) malloc(sizeof(unsigned) * table->rels_num * table->tups_num);

    /* Get the 2 relation rows ids vectors in referances */
    unsigned * row_ids_matrix       = table->row_ids;
    unsigned * new_row_ids_matrix   = new_table->row_ids;

    /* Get the 2 relations */
    Relation & relation_l        = getRelation(predicate_ptr->left.relId);
    Relation & relation_r        = getRelation(predicate_ptr->right.relId);

    /* Get their columns */
    uint64_t *column_values_l    = relation_l.columns[predicate_ptr->left.colId];
    uint64_t *column_values_r    = relation_r.columns[predicate_ptr->right.colId];

    /* Fint the indexes of the raltions in the table's */
    int index_l                  = -1;
    int index_r                  = -1;

    index_l = table->relations_bindings.find(predicate_ptr->left.binding)->second;
    index_r = table->relations_bindings.find(predicate_ptr->right.binding)->second;

    if (index_l == -1 || index_r == -1) std::cerr << "Error in SelfJoin: No mapping found for predicates" << '\n';

    /* Loop all the row_ids and keep the one's matching the predicate */
    unsigned rows_number = table->tups_num;
    unsigned rels_number = table->rels_num;
    unsigned new_tbi = 0;
    for (unsigned i = 0; i < rows_number; i++) {

        /* Apply the predicate: In case of success add to new table */
        if (column_values_l[row_ids_matrix[i*rels_number + index_l]] == column_values_r[row_ids_matrix[i*rels_number + index_r]]) {

            /* Add this row_id to all the relations */
            for (ssize_t relation = 0; relation < rels_number; relation++) {
                new_row_ids_matrix[new_tbi*rels_number  + relation] = row_ids_matrix[i*rels_number  + relation];

            }
            new_tbi++;
        }
    }

    new_table->tups_num = new_tbi;

    /*Delete old table_t */
    free(table->row_ids);
    delete table;

#ifdef time
    struct timeval end;
    gettimeofday(&end, NULL);
    timeSelfJoin += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
#endif

    return new_table;
}

/* The self Join Function */
void Joiner::noConstructSelfJoin(table_t *table, PredicateInfo *predicate_ptr, std::vector<SelectInfo> & selections) {

#ifdef hot
#ifdef time
    struct timeval start;
    gettimeofday(&start, NULL);
#endif

    /* Get the 2 relation rows ids vectors in referances */
    matrix &row_ids_matrix       = *(table->relations_row_ids);

    /* Get the 2 relations */
    Relation & relation_l        = getRelation(predicate_ptr->left.relId);
    Relation & relation_r        = getRelation(predicate_ptr->right.relId);

    /* Get their columns */
    uint64_t *column_values_l    = relation_l.columns[predicate_ptr->left.colId];
    uint64_t *column_values_r    = relation_r.columns[predicate_ptr->right.colId];

    /* Get their column's sizes */
    int column_size_l            = relation_l.size;
    int column_size_r            = relation_r.size;

    /* Fint the indexes of the raltions in the table's */
    int index_l                  = -1;
    int index_r                  = -1;
    int relations_num            = table->relations_bindings.size();

    for (ssize_t index = 0; index < relations_num ; index++) {
        if (predicate_ptr->left.binding == table->relations_bindings[index]) {
            index_l = index;
        }
        if (predicate_ptr->right.binding == table->relations_bindings[index]){
            index_r = index;
        }
    }

    if (index_l == -1 || index_r == -1) std::cerr << "Error in SelfJoin: No mapping found for predicates" << '\n';

    /* Calculate check sums on the fly , if its the last query */
    vector<uint64_t>  checksums(selections.size(), 0);
    //columns.resize(selections->size());
    //indexing.resize(selections->size());
    // for (SelectInfo sel: *selections) {
    //     if(table->relations_bindings[relation] == sel.binding) {
    //         uint64_t * col = getRelation(sel.relId).columns[sel.colId];
    //     }
    // }

    /* Loop all the row_ids and keep the one's matching the predicate */
    int rows_number = table->relations_row_ids->operator[](0).size();
    for (ssize_t i = 0; i < rows_number; i++) {

        /* Apply the predicate: In case of success add to new table */
        if (column_values_l[row_ids_matrix[index_l][i]] == column_values_r[row_ids_matrix[index_r][i]]) {

            /* Add this row_id to all the relations */
            for (ssize_t relation = 0; relation < relations_num; relation++) {

                /* Create checksums */
                int j = 0;
                for (SelectInfo sel: selections) {
                    if(table->relations_bindings[relation] == sel.binding) {
                        uint64_t * col = getRelation(sel.relId).columns[sel.colId];
                        checksums[j] += col[row_ids_matrix[relation][i]];
                    }
                    j++;
                }
            }
        }
    }

    /* Print the checksum */
    string result_str;
    for (size_t i = 0; i < checksums.size(); i++) {

        if (checksums[i] != 0)
            result_str += to_string(checksums[i]);
        else
            result_str += "NULL";

        // Create the write check sum
        if (i != checksums.size() - 1)
            result_str +=  " ";
    }
    cout << result_str << endl;

#ifdef time
    struct timeval end;
    gettimeofday(&end, NULL);
    timeSelfJoin += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
#endif

#endif
    return;
}

/* Its better hot to use it TODO change it */
void Joiner::Select(FilterInfo &fil_info, table_t* table) {

#ifdef time
    struct timeval start;
    gettimeofday(&start, NULL);
#endif

    /* Construct table  - Initialize variable */
    //(table->intermediate_res)? (construct(table)) : ((void)0);
    SelectInfo &sel_info = fil_info.filterColumn;
    uint64_t filter = fil_info.constant;

    if (fil_info.comparison == FilterInfo::Comparison::Less) {
        SelectLess(table, filter);
    } else if (fil_info.comparison == FilterInfo::Comparison::Greater) {
        SelectGreater(table, filter);
    } else if (fil_info.comparison == FilterInfo::Comparison::Equal) {
        SelectEqual(table, filter);
    }

#ifdef time
    struct timeval end;
    gettimeofday(&end, NULL);
    timeSelectFilter += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
#endif

}

void Joiner::SelectEqual(table_t *table, int filter) {
    /* Initialize helping variables */
    uint64_t *const values  = table->column_j->values;
    const unsigned table_index = table->column_j->table_index;
    const unsigned rel_num = table->rels_num;
    const unsigned size = table->tups_num;

    unsigned * old_row_ids = table->row_ids;
    unsigned * new_row_ids = (unsigned *) malloc(sizeof(unsigned) * size);  //TODO CHANGE HERE

    /* Update the row ids of the table */
    bool inter_res = table->intermediate_res;
    unsigned new_tbi = 0;

    /* Intermediate result */
    if (inter_res) {
        for (size_t index = 0; index < size; index++) {
            if (values[old_row_ids[index]] == filter) {
                new_row_ids[new_tbi] = old_row_ids[index];
                new_tbi++;
            }
        }
    }
    else {
        for (size_t index = 0; index < size; index++) {
            if (values[index] == filter) {
                new_row_ids[new_tbi] = index;
                new_tbi++;
            }
        }
        //ParallelNonItermediateFilterT pft( values, old_row_ids, new_row_ids, filter );
        //parallel_for(blocked_range<size_t>(0,size), pft);

    }

    /* Swap the old vector with the new one */
    (inter_res) ? (free(old_row_ids)) : ((void)0);
    table->row_ids = new_row_ids;
    table->tups_num = new_tbi;
    table->intermediate_res = true;
}

void Joiner::SelectGreater(table_t *table, int filter){

    /* Initialize helping variables */
    uint64_t *const values  = table->column_j->values;
    const unsigned table_index = table->column_j->table_index;
    const unsigned rel_num = table->rels_num;
    const unsigned size = table->tups_num;

    unsigned * old_row_ids = table->row_ids;
    unsigned * new_row_ids = (unsigned *) malloc(sizeof(unsigned) * size);  //TODO CHANGE HERE

    /* Update the row ids of the table */
    bool inter_res = table->intermediate_res;
    unsigned new_tbi = 0;
    if (inter_res) {
        for (size_t index = 0; index < size; index++) {
            if (values[old_row_ids[index]] > filter) {
                new_row_ids[new_tbi] = old_row_ids[index];
                new_tbi++;
            }
        }
    }
    else {
        for (size_t index = 0; index < size; index++) {
            if (values[index] > filter) {
                new_row_ids[new_tbi] = index;
                new_tbi++;
            }
        }
    }

    /* Swap the old vector with the new one */
    (inter_res) ? (free(old_row_ids)) : ((void)0);
    table->row_ids = new_row_ids;
    table->tups_num = new_tbi;
    table->intermediate_res = true;
}

void Joiner::SelectLess(table_t *table, int filter){
    /* Initialize helping variables */
    uint64_t *const values  = table->column_j->values;
    const unsigned table_index = table->column_j->table_index;
    const unsigned rel_num = table->rels_num;
    const unsigned size = table->tups_num;

    unsigned * old_row_ids = table->row_ids;
    unsigned * new_row_ids = (unsigned *) malloc(sizeof(unsigned) * size);  //TODO CHANGE HERE

    /* Update the row ids of the table */
    bool inter_res = table->intermediate_res;
    unsigned new_tbi = 0;
    if (inter_res) {
        for (size_t index = 0; index < size; index++) {
            if (values[old_row_ids[index]] < filter) {
                new_row_ids[new_tbi] = old_row_ids[index];
                new_tbi++;
            }
        }
    }
    else {
        for (size_t index = 0; index < size; index++) {
            if (values[index] < filter) {
                new_row_ids[new_tbi] = index;
                new_tbi++;
            }
        }
    }

    /* Swap the old vector with the new one */
    (inter_res) ? (free(old_row_ids)) : ((void)0);
    table->row_ids = new_row_ids;
    table->tups_num = new_tbi;
    table->intermediate_res = true;
}
