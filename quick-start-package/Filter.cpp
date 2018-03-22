#include "Joiner.hpp"


using namespace std;

double timeSelfJoin = 0;
double timeSelectFilter = 0;


/* The self Join Function */
table_t * Joiner::SelfJoin(table_t *table, PredicateInfo *predicate_ptr, std::vector<SelectInfo>* selections) {

#ifdef time
    struct timeval start;
    gettimeofday(&start, NULL);
#endif

    /* Create - Initialize a new table */
    table_t *new_table            = new table_t;
    new_table->relations_bindings = std::vector<unsigned>(table->relations_bindings);
    new_table->relations_row_ids  = new matrix;
    new_table->intermediate_res   = true;
    new_table->column_j           = new column_t;

    /* Get the 2 relation rows ids vectors in referances */
    matrix &row_ids_matrix       = *(table->relations_row_ids);
    matrix &new_row_ids_matrix   = *(new_table->relations_row_ids);

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

        /* Initialize the new matrix */
        new_row_ids_matrix.push_back(j_vector());
    }

    //if (index_l == -1 || index_r == -1) std::cerr << "Error in SelfJoin: No mapping found for predicates" << '\n';

    /* Loop all the row_ids and keep the one's matching the predicate */
    int rows_number = table->relations_row_ids->operator[](0).size();
    for (ssize_t i = 0; i < rows_number; i++) {

        /* Apply the predicate: In case of success add to new table */
        if (column_values_l[row_ids_matrix[index_l][i]] == column_values_r[row_ids_matrix[index_r][i]]) {

            /* Add this row_id to all the relations */
            for (ssize_t relation = 0; relation < relations_num; relation++) {
                new_row_ids_matrix[relation].push_back(row_ids_matrix[relation][i]);
            }
        }
    }

#ifdef time
    struct timeval end;
    gettimeofday(&end, NULL);
    timeSelfJoin += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
#endif

    /*Delete old table_t */
    //delete table->relations_row_ids;

    return new_table;
}

/* The self Join Function */
void Joiner::noConstructSelfJoin(table_t *table, PredicateInfo *predicate_ptr, std::vector<SelectInfo> & selections) {

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

    return;
}

/* Its better not to use it TODO change it */
void Joiner::Select(FilterInfo &fil_info, table_t* table) {

#ifdef time
    struct timeval start;
    gettimeofday(&start, NULL);
#endif

    /* Construct table  - Initialize variable */
    (table->intermediate_res)? (construct(table)) : ((void)0);
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
    int table_index         = table->column_j->table_index;
    const uint64_t rel_num  = table->relations_row_ids->size();

    matrix & old_row_ids = *table->relations_row_ids;
    const uint64_t size  = old_row_ids[table_index].size();
    matrix * new_row_ids = new matrix(rel_num);
    new_row_ids->at(0).reserve(size/2);

    /* Update the row ids of the table */
    for (size_t index = 0; index < size; index++) {
        if (values[index] == filter) {
            for (size_t rel_index = 0; rel_index < rel_num; rel_index++) {
                new_row_ids->operator[](rel_index).push_back(old_row_ids[rel_index][index]);
            }
        }
    }

    /* Swap the old vector with the new one */
    delete table->relations_row_ids;
    table->relations_row_ids = new_row_ids;
    table->intermediate_res = true;
}

void Joiner::SelectGreater(table_t *table, int filter){
    /* Initialize helping variables */
    uint64_t *const values  = table->column_j->values;
    int table_index         = table->column_j->table_index;
    const uint64_t rel_num  = table->relations_row_ids->size();

    matrix & old_row_ids = *table->relations_row_ids;
    const uint64_t size  = old_row_ids[table_index].size();
    matrix * new_row_ids = new matrix(rel_num);
    new_row_ids->at(0).reserve(size/2);

    /* Update the row ids of the table */
    for (size_t index = 0; index < size; index++) {
        if (values[index] > filter) {
            for (size_t rel_index = 0; rel_index < rel_num; rel_index++) {
                new_row_ids->operator[](rel_index).push_back(old_row_ids[rel_index][index]);
            }
        }
    }

    /* Swap the old vector with the new one */
    delete table->relations_row_ids;
    table->relations_row_ids = new_row_ids;
    table->intermediate_res = true;
}

void Joiner::SelectLess(table_t *table, int filter){
    /* Initialize helping variables */
    uint64_t *const values  = table->column_j->values;
    int table_index         = table->column_j->table_index;
    const uint64_t rel_num  = table->relations_row_ids->size();

    matrix & old_row_ids = *table->relations_row_ids;
    const uint64_t size  = old_row_ids[table_index].size();
    matrix * new_row_ids = new matrix(rel_num);
    new_row_ids->at(0).reserve(size/2);

    /* Update the row ids of the table */
    for (size_t index = 0; index < size; index++) {
        if (values[index] < filter) {
            for (size_t rel_index = 0; rel_index < rel_num; rel_index++) {
                new_row_ids->operator[](rel_index).push_back(old_row_ids[rel_index][index]);
            }
        }
    }

    /* Swap the old vector with the new one */
    delete table->relations_row_ids;
    table->relations_row_ids = new_row_ids;
    table->intermediate_res = true;
}
