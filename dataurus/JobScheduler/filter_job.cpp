#include "filter_job.h"

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
            args_.new_array[args_.prefix++] =  args_.old_rids[i*args_.rel_num + args_.table_index];;
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
        if (args_.values[args_.old_rids[i*args_.rel_num + args_.table_index]] > args_.filter) {
            args_.new_array[args_.prefix++] =  args_.old_rids[i*args_.rel_num + args_.table_index];;
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
            args_.new_array[args_.prefix++] =  args_.old_rids[i*args_.rel_num + args_.table_index];;
        }
    }

}


/*++++++++++++++++++++++++++++++++++++*/
/* NON INTERMERIATE PRALLEL FUNCTIONS */
/*++++++++++++++++++++++++++++++++++++*/

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
