#pragma once

typedef std::vector<std::vector<int>> matrix;
typedef struct table_t table_t;
typedef struct column_t column_t;
typedef struct hash_entry hash_entry;
typedef struct cartesian_product cartesian_product_t;

struct hash_entry {
    uint64_t row_id;
    uint64_t index;
};

struct column_t {
    uint64_t *values;
    uint64_t  size;
    unsigned  binding;
};

struct table_t {

    /* Row Ids and relation Ids */
    uint64_t                   **row_ids;

    /* use it for the filtrering TODO hash map ?*/
    std::vector<unsigned>      relation_ids;

    /* use the binfing to map the relations */
    std::vector<unsigned>      relations_bindings;

    /* Intermediate result falg */
    bool intermediate_res;

    int size_of_row_ids;
    int num_of_relations;

};


column_t *CreateColumn(SelectInfo& sel_info, Joiner& joiner);

