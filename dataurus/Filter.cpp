//#include "include/Filter_tbb_types.hpp"  UNCOMENT TO USE TBB
#include "Joiner.hpp"
#include "filter_job.h"
#include "parallel_radix_join.h"
#include "prj_params.h"
#include "generator.h"

/* --------------------------------------CACHED FILTERS------------------------------------- */
extern std::map<Selection, cached_t*> idxcache;
extern pthread_mutex_t cache_mtx;

static void *
alloc_aligned(size_t size)
{
    void * ret;
    int rv;
    rv = posix_memalign((void**)&ret, CACHE_LINE_SIZE, size);

    if (rv) {
        perror("alloc_aligned() failed: out of memory");
        return 0;
    }

    return ret;
}

/* #define RADIX_HASH(V)  ((V>>7)^(V>>13)^(V>>21)^V) */
#define HASH_BIT_MODULO(K, MASK, NBITS) (((K) & MASK) >> NBITS)



double timeSelfJoin = 0;
double timeSCSelfJoin = 0;
double timeSelectFilter = 0;
double timeIntermediateFilters = 0;
double timeNonIntermediateFilters = 0;
double timeEqualFilter = 0;
double timeLessFilter  = 0;
double timeGreaterFilter = 0;

#define RANGE           100
#define RANGE_SELF_JOIN 20

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
    new_table->ch_filter          = NULL;
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

    size_t range = RANGE_SELF_JOIN;
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



/* Its better hot to use it TODO change it */
// void Joiner::SelectAll(vector<FilterInfo*> & filterPtrs, table_t* table) {
//
//     /* Get the relation and the columns of the relation */
//     Relation &rel = getRelation(filterPtrs[0]->filterColumn.relId);
//     vector<uint64_t*> & columns = rel.columns;
//     unsigned   size = rel.size;
//     unsigned * new_row_ids = NULL;
//     unsigned   new_size = 0;
//
//     // create a bool array same as the size of the relations_num
//     bool * bitmap = (bool *) calloc(size, sizeof(bool));
//     size_t range  = RANGE;
//
//     // Call the right filter function
//     bool pass = false;
//     struct filters_arg a[range];
//     unsigned iter = 0;
//
//     // Create the args
//     for (size_t i = 0; i < range; i++) {
//         a[i].low   = (i < size % range) ? i * (size / range) + i : i * (size / range) + size % range;
//         a[i].high  = (i < size % range) ? a[i].low + size / range + 1 :  a[i].low + size / range;
//         a[i].values = NULL; // Will be changed inside
//         a[i].filter = 0;    // Will be chabged inside
//         a[i].prefix = 0;
//         a[i].size   = 0;
//         a[i].bitmap = bitmap;
//     }
//
//     // Pass the bitmap array from multiple filters
//     for (FilterInfo * filter : filterPtrs ) {
//
//         // Call the right filter function
//         if (filter->comparison == FilterInfo::Comparison::Less) {
//             for (size_t i = 0; i < range; i++) {
//                 if (a[i].size == 0 && iter != 0) continue;
//                 a[i].values = columns[filter->filterColumn.colId];
//                 a[i].filter = filter->constant;
//
//                 if (iter == 0) { // Call non intermediate filter
//                     job_scheduler.Schedule(new JobBitMapNonInterLessFiter(a[i]));
//                 } else {          // Call intermediate filter
//                     job_scheduler.Schedule(new JobBitMapInterLessFiter(a[i]));
//                 }
//             }
//         }
//         else if ((*filter).comparison == FilterInfo::Comparison::Greater) {
//             for (size_t i = 0; i < range; i++) {
//                 if (a[i].size == 0 && iter != 0) continue;
//                 a[i].values = columns[filter->filterColumn.colId];
//                 a[i].filter = filter->constant;
//
//                 if (iter == 0) { // Call non intermediate filter
//                     job_scheduler.Schedule(new JobBitMapNonInterGreaterFiter(a[i]));
//                 } else {          // Call intermediate filter
//                     job_scheduler.Schedule(new JobBitMapInterGreaterFiter(a[i]));
//                 }
//             }
//         }
//         else if ((*filter).comparison == FilterInfo::Comparison::Equal) {
//             for (size_t i = 0; i < range; i++) {
//                 if (a[i].size == 0 && iter != 0) continue;
//                 a[i].values = columns[filter->filterColumn.colId];
//                 a[i].filter = filter->constant;
//
//                 if (iter == 0) { // Call non intermediate filter
//                     job_scheduler.Schedule(new JobBitMapNonInterEqualFiter(a[i]));
//                 } else {          // Call intermediate filter
//                     job_scheduler.Schedule(new JobBitMapInterEqualFiter(a[i]));
//                 }
//             }
//         }
//
//         // Wait for the filter to end
//         iter++;
//         job_scheduler.Barrier();
//     }
//
//     // After bit map chunking construct the new tabl_t
//
//     /* Calculate the prefix sums */
//     new_size = a[0].size;
//     for (size_t i = 1; i < range; i++) {
//         a[i].prefix = new_size;
//         new_size   += a[i].size;
//         // std::cerr << "Prefix is :" << a[i].prefix << " size " << a[i].size <<'\n';
//     }
//
//     // malloc new values
//     new_row_ids = (unsigned *) malloc(sizeof(unsigned) * new_size);
//     for (size_t i = 0; i < range; i++) {
//         if (a[i].size == 0) continue;
//         //a[i].new_array = new_row_ids;
//         job_scheduler.Schedule(new JobBitMapCreate(a[i]));
//     }
//     job_scheduler.Barrier();
//
//     // /* Swap the old vector with the new one */
//     table->row_ids = new_row_ids;
//     table->tups_num = new_size;
//     table->intermediate_res = true;
// }

void Joiner::Select(FilterInfo &fil_info, table_t* table, ColumnInfo* columnInfo) {
    #ifdef time
    struct timeval start;
    gettimeofday(&start, NULL);
    #endif

    /* Construct table  - Initialize variable */
    SelectInfo &sel_info = fil_info.filterColumn;
    uint64_t filter = fil_info.constant;
    int ch_flag;

    /* Look Up to the map to see if is cached first */
    Selection sel(sel_info);
    pthread_mutex_lock(&cache_mtx);
    cache_res it = idxcache.find(sel);
    ch_flag = (it != idxcache.end());
    pthread_mutex_unlock(&cache_mtx);

    if (fil_info.comparison == FilterInfo::Comparison::Less) {
        /* CACHED */
        if (ch_flag == 1) {
            //fprintf(stderr, "CACHED LEES\n");
            //Cached_SelectLess(filter, it);
        }
        //else {
            SelectLess(table, filter);
            columnInfo->max = filter;
        //}
    }
    else if (fil_info.comparison == FilterInfo::Comparison::Greater) {
        /* CACHED */
        if (ch_flag == 1) {
            //fprintf(stderr, "CACHED GREATER\n");
            //Cached_SelectGreater(filter, it);
        }
        //else {
            SelectGreater(table, filter);
            columnInfo->min = filter;
        //}
    }
    else if (fil_info.comparison == FilterInfo::Comparison::Equal) {
        /* CACHED */
        // if (ch_flag == 1) {
        //     std::cerr << "Found cached for " << sel.relId << "." << sel.colId << '\n';
        //     //pthread_mutex_lock(&cache_mtx);
        //     table->ch_filter = Cached_SelectEqual(filter, it, table);
        //     //pthread_mutex_unlock(&cache_mtx);
        //
        //     /* Means we must not compute join */
        //     if(table->ch_filter == NULL) {
        //         table->tups_num = 0;
        //     }
        // }
        // else {
            SelectEqual(table, filter);
        //}
            columnInfo->min = filter;
            columnInfo->max = filter;

            // if (table->ch_filter != NULL) {
            //     std::cerr << "Normal " << table->tups_num << '\n';
            //     table->ch_filter = NULL;
            // }
    }

    #ifdef time
    struct timeval end;
    gettimeofday(&end, NULL);
    timeSelectFilter += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
    #endif
}

/* this must be implemented now */
cached_t* Joiner::Cached_SelectEqual(uint64_t fil, cache_res& cache_info, table_t* table) {
    /* Get the chached_t to filter the array we have */
    cached_t *ch_copy = (*cache_info).second;
    tuple_t  *ch_tmp  = (tuple_t *)ch_copy->tmp;

    /* Do the hash stuff */
    unsigned   num_tups = 0;
    const uint32_t numR = ch_copy->total_tuples;
    const int fanOut    = 1 << NUM_RADIX_BITS;
    const uint32_t MASK = fanOut-1;
    uint32_t idx        = HASH_BIT_MODULO(fil, MASK, 0);

    /* Copy things in the cached_t */
    /* Only output and tmp  */
    cached_t *ch_t    = (cached_t *) calloc(THREAD_NUM_1CPU, sizeof(cached_t));
    tuple_t  *tmp     = (tuple_t*)   alloc_aligned(ch_copy->total_tuples * sizeof(tuple_t) + RELATION_PADDING);
    int64_t  *output  = (int64_t *)  calloc((fanOut+1), sizeof(int64_t));

    // Make changes to the new one
    uint32_t count  = ch_copy->output[idx];
    uint32_t bound  = ch_copy->output[idx+1] - PADDING_TUPLES;
    for (uint32_t i = ch_copy->output[idx]; i < bound; i++) {
        if (ch_tmp[i].key == fil) {
            // Keep the wanted ones
            tmp[count].key     = ch_tmp[i].key;
            tmp[count].payload = ch_tmp[i].payload;
            count++;
        }
    }

    // for (size_t i = ch_copy->output[idx]; i < bound; i++) {
    //     fprintf(stderr, "temp{%d}=%lf.%d | ", i, tmp[i].key, tmp[i].payload);
    //     fprintf(stderr, "Old temp{%d}=%lf.%d\n", i, ch_tmp[i].key, ch_tmp[i].key);
    // }
    //
    // std::cerr << "Cached found " << count - ch_copy->output[idx] << '\n';

    // Copy old temp to new one
    // Create the right temp for the others
    for (int i = 0; i < THREAD_NUM_1CPU; i++) {
        ch_t[i]     = ch_copy[i];
        ch_t[i].tmp = tmp;
    }

    // Create the right output for thread 0
    output[idx]     = ch_copy->output[idx];
    output[idx + 1] = count + PADDING_TUPLES;
    ch_t[0].output = output;

    // Fill in the table's tuples
    table->tups_num = numR;


    /* All died from the filter */
    if (count == ch_copy->output[idx]) {
        free(tmp);
        free(output);
        free(ch_t);
        return NULL;
    }

    return ch_t;
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

    size_t range = RANGE;//getRange(THREAD_NUM, size);  // get a good range

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
    size_t range = RANGE; //getRange(THREAD_NUM, size);  // get a good range
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
    size_t range = RANGE;//getRange(THREAD_NUM, size);  // get a good range
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
