#include "filter_job.h"



// Give a good range depending on threads and size
int getRange(int threads, unsigned size) {
     // if (size < 10000)
     //     return threads/2;

    return threads;
}


// Self Join functions
int JobSelfJoin::Run() {
    for (size_t relation = args_.low; relation < args_.high; relation++) {
        args_.new_row_ids_matrix[args_.new_tbi*args_.rels_number + relation] = args_.row_ids_matrix[args_.i*args_.rels_number + relation];
    }
}

// No Construct Self Join functions
int JobNoConstrSelfJoinFindIdx::Run() {
    for (ssize_t index = args_.low; index < args_.high; index++) {
        if (args_.predicate_ptr->left.binding == args_.table->relations_bindings[index]) {
            args_.index_l = index;
        }
        if (args_.predicate_ptr->right.binding == args_.table->relations_bindings[index]){
            args_.index_r = index;
        }
        if (args_.index_l != -1 && args_.index_r != -1) break;
    }
}

int JobNoConstrSelfJoinKeepRowIds::Run() {
    for (ssize_t i = args_.low; i < args_.high; i++) {
        /* Apply the predicate: In case of success add to new table */
        if (args_.column_values_l[args_.row_ids_matrix[args_.index_l][i]] == args_.column_values_r[args_.row_ids_matrix[args_.index_r][i]]) {
            /* Add this row_id to all the relations */
            for (ssize_t relation = 0; relation < args_.relations_num; relation++) {
                /* Create checksums */
                int j = 0;
                for (SelectInfo sel: args_.selections) {
                    if(args_.table->relations_bindings[relation] == sel.binding) {
                        uint64_t * col = args_.joinerPtr->getRelation(sel.relId).columns[sel.colId];
                        args_.checksums[j] += col[args_.row_ids_matrix[relation][i]];
                    }
                    j++;
                }
            }
        }
    }
}

int JobNoConstrSelfJoinChecksum::Run() {
    for (size_t i = args_.low; i < args_.high; i++) {
        if (args_.checksums[i] != 0)
            args_.local_result_str += to_string(args_.checksums[i]);
        else
            args_.local_result_str += "NULL";

        // Create the write check sum
        if (i != args_.checksums.size() - 1)
            args_.local_result_str +=  " ";
    }
}


// Less Intermediate Filter functions
int JobLessInterFindSize::Run() {
    for (size_t i = args_.low; i < args_.high; i++) {
        if (args_.values[args_.old_rids[i*args_.rel_num + args_.table_index]] < args_.filter)
            args_.prefix++;
    }
}

int JobLessInterFilter::Run() {
    for (size_t i = args_.low; i < args_.high; i++) {
        if (args_.values[args_.old_rids[i*args_.rel_num + args_.table_index]] < args_.filter) {
            for (size_t j = 0; j < args_.rel_num; j++){
                args_.new_array[args_.prefix*args_.rel_num + j] = args_.old_rids[i*args_.rel_num + j];
            }
            args_.prefix++;
        }
    }

}

// Greater Intermediate Filter functions
int JobGreaterInterFindSize::Run() {
    for (size_t i = args_.low; i < args_.high; i++) {
        if (args_.values[args_.old_rids[i*args_.rel_num + args_.table_index]] > args_.filter)
            args_.prefix++;
    }
}

int JobGreaterInterFilter::Run() {
    for (size_t i = args_.low; i < args_.high; i++) {
        if (args_.values[args_.old_rids[i*args_.rel_num + args_.table_index]] > args_.filter){
            for (size_t j = 0; j < args_.rel_num; j++){
                args_.new_array[args_.prefix*args_.rel_num + j] = args_.old_rids[i*args_.rel_num + j];
            }
            args_.prefix++;
        }
    }
}

// Equal Intermediate Filter functions
int JobEqualInterFindSize::Run() {
    for (size_t i = args_.low; i < args_.high; i++) {
        if (args_.values[args_.old_rids[i*args_.rel_num + args_.table_index]] == args_.filter)
            args_.prefix++;
    }
}

int JobEqualInterFilter::Run() {
    for (size_t i = args_.low; i < args_.high; i++) {
        if (args_.values[args_.old_rids[i*args_.rel_num + args_.table_index]] == args_.filter) {
            for (size_t j = 0; j < args_.rel_num; j++){
                args_.new_array[args_.prefix*args_.rel_num + j] = args_.old_rids[i*args_.rel_num + j];
            }
            args_.prefix++;
        }
    }

}


/*++++++++++++++++++++++++++++++++++++*/
/* NON INTERMERIATE PRALLEL FUNCTIONS */
/*++++++++++++++++++++++++++++++++++++*/

// All filter Run functions
int JobAllNonInterFindSize::Run() {
    for (size_t i = args_.low; i < args_.high; i++) {

        /* Loop for all the predicates */
        /* Loop for all the predicates */
        bool pass;
        for (auto filter : (*args_.filterPtrs)) {
            pass = false;

            /* If it passes all the filter */
            if ((*filter).comparison == FilterInfo::Comparison::Less)
                pass = (*args_.columns)[(*filter).filterColumn.colId][i] < (*filter).constant ? true : false;
            else if ((*filter).comparison == FilterInfo::Comparison::Greater)
                pass = (*args_.columns)[(*filter).filterColumn.colId][i] > (*filter).constant? true : false;
            else if ((*filter).comparison == FilterInfo::Comparison::Equal)
                pass = (*args_.columns)[(*filter).filterColumn.colId][i] == (*filter).constant ? true : false;

            if (!pass) break;
        }

        /* Add it if pass == true */
        if (pass) args_.prefix++;
    }
}

int JobAllNonInterFilter::Run() {
    for (size_t i = args_.low; i < args_.high; i++) {

        /* Loop for all the predicates */
        bool pass;
        for (auto filter : (*args_.filterPtrs)) {
            pass = false;

            /* If it passes all the filter */
            if ((*filter).comparison == FilterInfo::Comparison::Less)
                pass = (*args_.columns)[(*filter).filterColumn.colId][i] < (*filter).constant ? true : false;
            else if ((*filter).comparison == FilterInfo::Comparison::Greater)
                pass = (*args_.columns)[(*filter).filterColumn.colId][i] > (*filter).constant? true : false;
            else if ((*filter).comparison == FilterInfo::Comparison::Equal)
                pass = (*args_.columns)[(*filter).filterColumn.colId][i] == (*filter).constant ? true : false;

            if (!pass) break;
        }

        /* Add it if pass == true */
        if (pass) args_.new_array[args_.prefix++] = i;
    }
}


// Less filter Run functions
int JobLessNonInterFindSize::Run() {

    for (size_t i = args_.low; i < args_.high; i++) {
        if (args_.values[i] < args_.filter)
            args_.prefix++;
    }
}

int JobLessNonInterFilter::Run() {
    for (size_t i = args_.low; i < args_.high; i++) {
        if (args_.values[i] < args_.filter)
            args_.new_array[args_.prefix++] = i;
    }
}

// Greater filter Run functions
int JobGreaterNonInterFindSize::Run() {
    for (size_t i = args_.low; i < args_.high; i++) {
        if (args_.values[i] > args_.filter)
            args_.prefix++;
    }
}

int JobGreaterNonInterFilter::Run() {
    for (size_t i = args_.low; i < args_.high; i++) {
        if (args_.values[i] > args_.filter)
            args_.new_array[args_.prefix++] = i;
    }
}

// Equal filter Run functions
int JobEqualNonInterFindSize::Run() {
    for (size_t i = args_.low; i < args_.high; i++) {
        if (args_.values[i] == args_.filter)
            args_.prefix++;
    }
}

int JobEqualNonInterFilter::Run() {
    for (size_t i = args_.low; i < args_.high; i++) {
        if (args_.values[i] == args_.filter)
            args_.new_array[args_.prefix++] = i;
    }
}
