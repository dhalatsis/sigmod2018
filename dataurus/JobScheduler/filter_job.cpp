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
