//#include "include/Filter_tbb_types.hpp"  UNCOMENT TO USE TBB
#include "Joiner.hpp"
#include "filter_job.h"

double timeSelfJoin = 0;
double timeSCSelfJoin = 0;
double timeSelectFilter = 0;
double timeIntermediateFilters = 0;
double timeNonIntermediateFilters = 0;
double timeEqualFilter = 0;
double timeLessFilter  = 0;
double timeGreaterFilter = 0;

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

    //if (index_l == -1 || index_r == -1) std::cerr << "Error in SelfJoin: No mapping found for predicates" << '\n';

    /* Loop all the row_ids and keep the one's matching the predicate */
    unsigned rows_number = table->tups_num;
    unsigned rels_number = table->rels_num;
    unsigned new_size = 0;

    size_t range = THREAD_NUM_1CPU + THREAD_NUM_2CPU;
    struct self_join_arg a[range];
    // struct self_join_arg * a = (self_join_arg *) malloc(range * sizeof(self_join_arg));
    for (size_t i = 0; i < range; i++) {
        a[i].low   = (i < rows_number % range) ? i * (rows_number / range) + i : i * (rows_number / range) + rows_number % range;
        a[i].high  = (i < rows_number % range) ? a[i].low + rows_number / range + 1 :  a[i].low + rows_number / range;
        a[i].column_values_l = column_values_l;
        a[i].column_values_r = column_values_r;
        a[i].index_l = index_l;
        a[i].index_r = index_r;
        a[i].row_ids_matrix = row_ids_matrix;
        a[i].new_row_ids_matrix = new_row_ids_matrix;
        a[i].rels_number = rels_number;
        a[i].prefix = 0;
        a[i].size = 0;
        job_scheduler.Schedule(new JobSelfJoinFindSize(a[i]));
    }
    job_scheduler.Barrier();

    /* Calculate the prefix sums */
    unsigned temp = 0;
    for (size_t i = 0; i < range; i++) {
        a[i].prefix += temp;
        temp += a[i].size;
    }
    new_size = temp;

    new_row_ids_matrix = (unsigned *) malloc(sizeof(unsigned) * rels_number * new_size);
    for (size_t i = 0; i < range; i++) {
        if (a[i].size == 0) continue;
        a[i].new_row_ids_matrix = new_row_ids_matrix;
        job_scheduler.Schedule(new JobSelfJoin(a[i]));
    }
    job_scheduler.Barrier();

    new_table->row_ids = new_row_ids_matrix;
    new_table->tups_num = new_size;

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
table_t * Joiner::SelfJoinCheckSumOnTheFly(table_t *table, PredicateInfo *predicate_ptr, columnInfoMap & cmap, std::vector<SelectInfo> selections, string & result_str) {

#ifdef time
    struct timeval start;
    gettimeofday(&start, NULL);
#endif

    /* Get the 2 relation rows ids vectors in referances */
    unsigned * row_ids_matrix       = table->row_ids;

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

    //if (index_l == -1 || index_r == -1) std::cerr << "Error in SelfJoin: No mapping found for predicates" << '\n';
    unsigned rows_number = table->tups_num;
    unsigned rels_number = table->rels_num;

    /* Crete a vector for the pairs Column, Index in relationR/S */
    vector<struct checksumST> distinctPairs;
    struct checksumST st;

    /* take the distinct columns in a vector */
    unordered_map<unsigned, unsigned>::iterator itr;
    unsigned index = 0;
    for (columnInfoMap::iterator it=cmap.begin(); it != cmap.end(); it++) {
        index = -1;
        itr = table->relations_bindings.find(it->first.binding);
        if (itr != table->relations_bindings.end()) {
            st.colId = it->first.colId;
            st.binding = it->first.binding;
            st.index = itr->second;
            st.values = getRelation(it->first.relId).columns[st.colId];
            distinctPairs.push_back(st);
        }
    }

    // Range for chunking
    size_t range = THREAD_NUM_1CPU + THREAD_NUM_2CPU;

    /* Calculate check sums on the fly , if its the last query */
    vector<uint64_t> sum(distinctPairs.size(), 0);
    vector<uint64_t*> sums(range);
    for (size_t i = 0; i < sums.size(); i++) {
        sums[i] = (uint64_t *) calloc (sum.size(), sizeof(uint64_t));
    }

    struct selfJoinSum_arg a[range];
    // struct selfJoinSum_arg * a = (selfJoinSum_arg *) malloc(range * sizeof(selfJoinSum_arg));
    for (size_t i = 0; i < range; i++) {
        a[i].low   = (i < rows_number % range) ? i * (rows_number / range) + i : i * (rows_number / range) + rows_number % range;
        a[i].high  = (i < rows_number % range) ? a[i].low + rows_number / range + 1 :  a[i].low + rows_number / range;
        a[i].column_values_l = column_values_l;
        a[i].column_values_r = column_values_r;
        a[i].row_ids_matrix = row_ids_matrix;
        a[i].index_l = index_l;
        a[i].index_r = index_r;
        a[i].relations_num = rels_number;
        a[i].priv_checsums = sums[i];
        a[i].distinctPairs = &distinctPairs;
        job_scheduler.Schedule(new JobCheckSumSelfJoin(a[i]));
    }
    job_scheduler.Barrier();


    /* Create the checksum */
    for (size_t j = 0; j < sum.size(); j++) {
        for (size_t i = 0; i < sums.size(); i++) {
            sum[j] += sums[i][j];
        }
    }

    /* Construct the checksums in the right way */
    bool found = false;
    for (size_t i = 0; i < selections.size(); i++) {

        // Look up the check sum int the array
        for (size_t j = 0; j < distinctPairs.size(); j++) {
            if (selections[i].colId == distinctPairs[j].colId
                && selections[i].binding == distinctPairs[j].binding)
            {
                (sum[j] == 0) ? result_str += "NULL" : result_str += to_string(sum[j]);
                found = true;
                break;
            }
        }

        // Create the write check sum
        if (i != selections.size() - 1) {
            result_str +=  " ";
        }
    }

#ifdef time
    struct timeval end;
    gettimeofday(&end, NULL);
    timeSCSelfJoin += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
#endif

    return NULL;

}


/* Its better hot to use it TODO change it */
void Joiner::SelectAll(vector<FilterInfo*> & filterPtrs, table_t* table) {

#ifdef time
    struct timeval start;
    gettimeofday(&start, NULL);
#endif

    /* Get the relation and the columns of the relation */
    Relation &rel = getRelation(filterPtrs[0]->filterColumn.relId);
    vector<uint64_t*> & columns = rel.columns;
    unsigned size = rel.size;

    unsigned * old_row_ids = table->row_ids;
    unsigned * new_row_ids = NULL;  //TODO CHANGE HERE

    bool inter_res = table->intermediate_res;
    unsigned new_tbi = 0;

    size_t range = THREAD_NUM_1CPU;
    /* Intermediate result */
    if (inter_res) {

    }
    else {
        struct allfilters_arg a[range];
        // struct allfilters_arg * a = (allfilters_arg *) malloc(range * sizeof(allfilters_arg));
        for (size_t i = 0; i < range; i++) {
            a[i].low   = (i < size % range) ? i * (size / range) + i : i * (size / range) + size % range;
            a[i].high  = (i < size % range) ? a[i].low + size / range + 1 :  a[i].low + size / range;
            a[i].columns = & columns;
            a[i].filterPtrs = & filterPtrs;
            a[i].prefix = 0;
            a[i].size = 0;
            job_scheduler.Schedule(new JobAllNonInterFindSize(a[i]));
        }
        job_scheduler.Barrier();

        /* Calculate the prefix sums */
        new_tbi += a[0].size;
        for (size_t i = 1; i < range; i++) {
            a[i].prefix = a[i-1].size;
            //std::cerr << "Prefix is :" << a[i].prefix << '\n';
            new_tbi += a[i].size;
        }

        // malloc new values
        new_row_ids = (unsigned *) malloc(sizeof(unsigned) * new_tbi);
        for (size_t i = 0; i < range; i++) {
            a[i].new_array = new_row_ids;
            job_scheduler.Schedule(new JobAllNonInterFilter(a[i]));
        }
        job_scheduler.Barrier();

        // free(a);    // TODO reconsider
    }

    /* Swap the old vector with the new one */
    (table->intermediate_res) ? (free(old_row_ids)) : ((void)0);
    table->row_ids = new_row_ids;
    table->tups_num = new_tbi;
    table->intermediate_res = true;

#ifdef time
    struct timeval end;
    gettimeofday(&end, NULL);
    timeSelectFilter += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
#endif
}

void Joiner::Select(FilterInfo &fil_info, table_t* table, ColumnInfo* columnInfo) {
    #ifdef time
    struct timeval start;
    gettimeofday(&start, NULL);
    #endif

    /* Construct table  - Initialize variable */
    SelectInfo &sel_info = fil_info.filterColumn;
    uint64_t filter = fil_info.constant;

    if (fil_info.comparison == FilterInfo::Comparison::Less) {
        SelectLess(table, filter);
        columnInfo->max = filter;
    }
    else if (fil_info.comparison == FilterInfo::Comparison::Greater) {
        SelectGreater(table, filter);
        columnInfo->min = filter;
    }
    else if (fil_info.comparison == FilterInfo::Comparison::Equal) {
        SelectEqual(table, filter);
        columnInfo->min = filter;
        columnInfo->max = filter;
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
    unsigned * new_row_ids = NULL;

    /* Update the row ids of the table */
    bool inter_res = table->intermediate_res;
    unsigned new_tbi = 0;

    size_t range = THREAD_NUM_1CPU + THREAD_NUM_2CPU  + 10;//getRange(THREAD_NUM, size);  // get a good range

    /* Intermediate result */
    if (inter_res) {

        #ifdef time
        struct timeval start;
        gettimeofday(&start, NULL);
        #endif

        struct inter_arg a[range];
        // struct inter_arg * a = (inter_arg *) malloc(range * sizeof(inter_arg));
        for (size_t i = 0; i < range; i++) {
            a[i].low   = (i < size % range) ? i * (size / range) + i : i * (size / range) + size % range;
            a[i].high  = (i < size % range) ? a[i].low + size / range + 1 :  a[i].low + size / range;
            a[i].values = values;
            a[i].filter = filter;
            a[i].old_rids = old_row_ids;
            a[i].rel_num = rel_num;
            a[i].table_index = table_index;
            a[i].prefix = 0;
            a[i].size = 0;
            job_scheduler.Schedule(new JobEqualInterFindSize(a[i]));
        }
        job_scheduler.Barrier();

        /* Calculate the prefix sums */
        new_tbi += a[0].size;
        unsigned temp = a[0].size;
        for (size_t i = 1; i < range; i++) {
            a[i].prefix = temp;
            temp += a[i].size;
        }
        new_tbi = temp;

        // malloc new values
        new_row_ids = (unsigned *) malloc(sizeof(unsigned) * new_tbi * rel_num);
        for (size_t i = 0; i < range; i++) {
            if (a[i].size == 0) continue;
            a[i].new_array = new_row_ids;
            job_scheduler.Schedule(new JobEqualInterFilter(a[i]));
        }
        job_scheduler.Barrier();
        #ifdef time
        struct timeval end;
        gettimeofday(&end, NULL);
        timeNonIntermediateFilters += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
        #endif

    }
    else {

        #ifdef time
        struct timeval start;
        gettimeofday(&start, NULL);
        #endif

        struct noninter_arg a[range];
        // struct noninter_arg * a = (noninter_arg *) malloc(range * sizeof(noninter_arg));
        for (size_t i = 0; i < range; i++) {
            a[i].low   = (i < size % range) ? i * (size / range) + i : i * (size / range) + size % range;
            a[i].high  = (i < size % range) ? a[i].low + size / range + 1 :  a[i].low + size / range;
            a[i].values = values;
            a[i].filter = filter;
            a[i].prefix = 0;
            a[i].size = 0;
            job_scheduler.Schedule(new JobEqualNonInterFindSize(a[i]));
        }
        job_scheduler.Barrier();

        /* Calculate the prefix sums */
        new_tbi += a[0].size;
        unsigned temp = a[0].size;
        for (size_t i = 1; i < range; i++) {
            a[i].prefix = temp;
            temp += a[i].size;
        }
        new_tbi = temp;

        // malloc new values
        new_row_ids = (unsigned *) malloc(sizeof(unsigned) * new_tbi);
        for (size_t i = 0; i < range; i++) {
            if (a[i].size == 0) continue;
            a[i].new_array = new_row_ids;
            job_scheduler.Schedule(new JobEqualNonInterFilter(a[i]));
        }
        job_scheduler.Barrier();

        // free(a);    // TODO reconsider

        #ifdef time
        struct timeval end;
        gettimeofday(&end, NULL);
        timeNonIntermediateFilters += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
        #endif

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
    unsigned * new_row_ids = NULL; //(unsigned *) malloc(sizeof(unsigned) * size);

    /* Update the row ids of the table */
    bool inter_res = table->intermediate_res;
    unsigned new_tbi = 0;
    size_t range = THREAD_NUM_1CPU + THREAD_NUM_2CPU  + 10;//getRange(THREAD_NUM, size);  // get a good range
    if (inter_res) {
        #ifdef time
        struct timeval start;
        gettimeofday(&start, NULL);
        #endif

        struct inter_arg a[range];
        // struct inter_arg * a = (inter_arg *) malloc(range * sizeof(inter_arg));
        for (size_t i = 0; i < range; i++) {
            a[i].low   = (i < size % range) ? i * (size / range) + i : i * (size / range) + size % range;
            a[i].high  = (i < size % range) ? a[i].low + size / range + 1 :  a[i].low + size / range;
            a[i].values = values;
            a[i].filter = filter;
            a[i].old_rids = old_row_ids;
            a[i].rel_num = rel_num;
            a[i].table_index = table_index;
            a[i].prefix = 0;
            a[i].size = 0;
            job_scheduler.Schedule(new JobGreaterInterFindSize(a[i]));
        }
        job_scheduler.Barrier();

        /* Calculate the prefix sums */
        new_tbi += a[0].size;
        unsigned temp = a[0].size;
        for (size_t i = 1; i < range; i++) {
            a[i].prefix = temp;
            temp += a[i].size;
        }
        new_tbi = temp;

        // malloc new values
        new_row_ids = (unsigned *) malloc(sizeof(unsigned) * new_tbi * rel_num);
        for (size_t i = 0; i < range; i++) {
            if (a[i].size == 0) continue;
            a[i].new_array = new_row_ids;
            job_scheduler.Schedule(new JobGreaterInterFilter(a[i]));
        }
        job_scheduler.Barrier();

        #ifdef time
        struct timeval end;
        gettimeofday(&end, NULL);
        timeIntermediateFilters += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
        #endif

    }
    else {

        #ifdef time
        struct timeval start;
        gettimeofday(&start, NULL);
        #endif

        struct noninter_arg a[range];
        // struct noninter_arg * a = (noninter_arg *) malloc(range * sizeof(noninter_arg));
        for (size_t i = 0; i < range; i++) {
            a[i].low   = (i < size % range) ? i * (size / range) + i : i * (size / range) + size % range;
            a[i].high  = (i < size % range) ? a[i].low + size / range + 1 :  a[i].low + size / range;
            a[i].values = values;
            a[i].filter = filter;
            a[i].prefix = 0;
            a[i].size = 0;
            job_scheduler.Schedule(new JobGreaterNonInterFindSize(a[i]));
        }
        job_scheduler.Barrier();

        new_tbi += a[0].size;
        unsigned temp = a[0].size;
        for (size_t i = 1; i < range; i++) {
            a[i].prefix = temp;
            temp += a[i].size;
            flush(cerr);
        }
        new_tbi = temp;

        // malloc new values
        new_row_ids = (unsigned *) malloc(sizeof(unsigned) * new_tbi);
        for (size_t i = 0; i < range ; i++) {
            if (a[i].size == 0) continue;
            a[i].new_array = new_row_ids;
            job_scheduler.Schedule(new JobGreaterNonInterFilter(a[i]));
        }
        job_scheduler.Barrier();

        // free(a);    // TODO reconsider

        #ifdef time
        struct timeval end;
        gettimeofday(&end, NULL);
        timeNonIntermediateFilters += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
        #endif
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
    unsigned * new_row_ids = NULL;

    /* Update the row ids of the table */
    bool inter_res = table->intermediate_res;
    unsigned new_tbi = 0;
    size_t range = THREAD_NUM_1CPU + THREAD_NUM_2CPU + 10;//getRange(THREAD_NUM, size);  // get a good range
    if (inter_res) {
        #ifdef time
        struct timeval start;
        gettimeofday(&start, NULL);
        #endif

        struct inter_arg a[range];
        // struct inter_arg * a = (inter_arg *) malloc(range * sizeof(inter_arg));
        for (size_t i = 0; i < range; i++) {
            a[i].low   = (i < size % range) ? i * (size / range) + i : i * (size / range) + size % range;
            a[i].high  = (i < size % range) ? a[i].low + size / range + 1 :  a[i].low + size / range;
            a[i].values = values;
            a[i].filter = filter;
            a[i].old_rids = old_row_ids;
            a[i].rel_num = rel_num;
            a[i].table_index = table_index;
            a[i].prefix = 0;
            a[i].size = 0;
            job_scheduler.Schedule(new JobLessInterFindSize(a[i]));
        }
        job_scheduler.Barrier();

        /* Calculate the prefix sums */
        new_tbi += a[0].size;
        unsigned temp = a[0].size;
        for (size_t i = 1; i < range; i++) {
            a[i].prefix = temp;
            temp += a[i].size;
        }
        new_tbi = temp;

        // malloc new values
        new_row_ids = (unsigned *) malloc(sizeof(unsigned) * new_tbi * rel_num);
        for (size_t i = 0; i < range; i++) {
            if (a[i].size == 0) continue;
            a[i].new_array = new_row_ids;
            job_scheduler.Schedule(new JobLessInterFilter(a[i]));
        }
        job_scheduler.Barrier();

        #ifdef time
        struct timeval end;
        gettimeofday(&end, NULL);
        timeIntermediateFilters += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
        #endif
    }
    else {
        #ifdef time
        struct timeval start;
        gettimeofday(&start, NULL);
        #endif

        struct noninter_arg a[range];
        // struct noninter_arg * a = (noninter_arg *) malloc(range * sizeof(noninter_arg));
        for (size_t i = 0; i < range; i++) {
            a[i].low   = (i < size % range) ? i * (size / range) + i : i * (size / range) + size % range;
            a[i].high  = (i < size % range) ? a[i].low + size / range + 1 :  a[i].low + size / range;
            a[i].values = values;
            a[i].filter = filter;
            a[i].prefix = 0;
            a[i].size = 0;
            job_scheduler.Schedule(new JobLessNonInterFindSize(a[i]));
        }
        job_scheduler.Barrier();

        /* Calculate the prefix sums */
        new_tbi += a[0].size;
        unsigned temp = a[0].size;
        for (size_t i = 1; i < range; i++) {
            a[i].prefix = temp;
            temp += a[i].size;
        }
        new_tbi = temp;

        // malloc new values
        new_row_ids = (unsigned *) malloc(sizeof(unsigned) * new_tbi);
        for (size_t i = 0; i < range; i++) {
            if (a[i].size == 0) continue;
            a[i].new_array = new_row_ids;
            job_scheduler.Schedule(new JobLessNonInterFilter(a[i]));
        }
        job_scheduler.Barrier();

        // free(a);    // TODO reconsider

        #ifdef time
        struct timeval end;
        gettimeofday(&end, NULL);
        timeNonIntermediateFilters += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
        #endif
    }

    /* Swap the old vector with the new one */
    (inter_res) ? (free(old_row_ids)) : ((void)0);
    table->row_ids = new_row_ids;
    table->tups_num = new_tbi;
    table->intermediate_res = true;
}
