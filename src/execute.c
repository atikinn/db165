#include <string.h>
#include <assert.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>

#include "parse.h"
#include "symtable.h"
#include "dbo.h"
#include "message.h"
#include "utils.h"
#include "vector.h"
#include "sindex.h"
#include "btree.h"

#define DEFAULT_TABLE_COUNT 8

static
struct db *create_db(char *db_name) {
    struct db *db = malloc(sizeof *db);
    if (db == NULL) return NULL;
    db->name = db_name;
    db->table_count = 0;
    db->tables = malloc(sizeof(struct table) * DEFAULT_TABLE_COUNT);
    assert(db->tables);
    db->capacity = DEFAULT_TABLE_COUNT;
    return db;
}

static
struct table *create_table(db* db, char* name, size_t max_cols) {
    struct table t = { .name = name, .length = 0, .clustered = max_cols, .col_count = 0 };
    t.col = malloc(sizeof(struct column) * max_cols);
    assert(t.col);

    // TODO: add the logic for resizing; max tables are 7 in tests
    db->tables[db->table_count] = t;
    return &db->tables[db->table_count++];;
}

static
struct column *create_column(table *table, char* name, bool sorted) {
    struct column c = { .name = name, .index = NULL, .table = table, .clustered = false };
    vector_init(&c.data, 16);
    assert(c.data.vals);
    if (sorted) {
        table->clustered = table->col_count;
        c.clustered = true;
    }
    table->col[table->col_count] = c;
    return &table->col[table->col_count++];
}

static
void create_index(struct column *col, enum index_type type) {
    switch(type) {
        case SORTED:
            col->index = malloc(sizeof *col->index);
            assert(col->index);
            col->index->type = type;
            col->index->index = sindex_create(&col->data);
            return;
        case BTREE:
            col->index = malloc(sizeof *col->index);
            assert(col->index);
            col->index->type = type;
            col->index->index = btree_create(col->clustered);
            btree_load(col->index->index, &col->data);
            return;
        case IDX_INVALID: assert(false);
    }
}

static status create(db_operator *query) {
    status st;
    db *db;
    table *tbl;
    column *col;
    bool ret;
    switch(query->create_type) {
        case(CREATE_DB):
            db = create_db(query->create_name);
            ret = db_set_current(db);
            st.code = ret ? OK : ERROR;
            st.message = "database created and set";
            break;
        case(CREATE_TBL):
            tbl = create_table(query->db, query->create_name, query->table_size);
            ret = map_insert(query->assign_var, tbl, ENTITY);
            st.code = ret ? OK : ERROR;
            st.message = "table created";
            cs165_log(stderr, "%s %s\n", st.message, query->create_name);
            break;
        case(CREATE_COL):
            col = create_column(query->tables, query->create_name, query->sorted);
            ret = map_insert(query->assign_var, col, ENTITY);
            cs165_log(stderr, "inserted %s with name %s\n", query->assign_var, col->name);
            st.code = ret ? OK : ERROR;
            st.message = "column created";
            cs165_log(stderr, "%s %s\n", st.message, query->create_name);
            break;
        case(CREATE_IDX):
            create_index(query->columns, query->idx_type);
            break;
        default: break;
    }
    return st;
}

//////////////////////////////////////////////////////////////////////////////

static
void index_insert(struct column *c, int val, size_t pos, size_t sz) {
    if (c->index == NULL) return;
    switch(c->index->type) {
        case BTREE:
            btree_insert(c->index->index, val, pos);
            return;
        case SORTED:
            c->index->index = sindex_insert(c->index->index, pos, val, sz);
            return;
        case IDX_INVALID: assert(false);
    }
}

static
status rel_insert(struct table *tbl, int *row) {
    status st;

    if (tbl->clustered < tbl->col_count) {
        size_t pos = vector_insert_sorted(&tbl->col[tbl->clustered].data, row[tbl->clustered]);
        for (unsigned j = 0; j < tbl->col_count; j++) {
            if (j != tbl->clustered) vector_insert(&tbl->col[j].data, row[j], pos);
            index_insert(&tbl->col[j], row[j], pos, tbl->length);
        }
    } else {
        for (unsigned j = 0; j < tbl->col_count; j++) {
            vector_insert(&tbl->col[j].data, row[j], tbl->length);
            index_insert(&tbl->col[j], row[j], tbl->length, tbl->length);
        }
    }

    tbl->length++;

    st.code = OK;
    st.message = "insert completed";
    return st;
}

//////////////////////////////////////////////////////////////////////////////
// TODO: binary tree integration: search
static
int *sort_clustered(struct column *col) {
    struct sindex *zip = sindex_create(&col->data);

    int *idxs = malloc(col->data.sz * sizeof *idxs);
    for (size_t j = 0; j < col->data.sz; j++) {
        col->data.vals[j] = zip[j].val;
        idxs[j] = zip[j].pos;
    }

    free(zip);
    return idxs;
}

static inline
void align_column(struct column *col, int *row, int n) {
    int *indices = malloc(n * sizeof *indices);
    memcpy(indices, row, n * sizeof *indices);

    int *a = col->data.vals;
    int i_src, i_dst;
    for (int i_dst_first = 0; i_dst_first < n; i_dst_first++) {
        i_src = indices[i_dst_first];       /* Check if this element needs to be permuted */
        assert(i_src < n);

        if (i_src == i_dst_first) continue; /* This element is already in place */

        i_dst = i_dst_first;
        int pending = a[i_dst];

        do {                                /* Follow the permutation cycle */
            a[i_dst] = a[i_src];
            indices[i_dst] = i_dst;

            i_dst = i_src;
            i_src = indices[i_src];
            assert(i_src != i_dst);

        } while (i_src != i_dst_first);

        a[i_dst] = pending;
        indices[i_dst] = i_dst;
    }

    free(indices);
}

static inline
void bulk_rel_insert(struct table *tbl, int *row) {
    for (unsigned j = 0; j < tbl->col_count; j++)
        vector_push(&tbl->col[j].data, row[j]);
    tbl->length++;
}

static
void mk_cluster(struct table *tbl) {
    int *alignment = sort_clustered(&tbl->col[tbl->clustered]);

    for (size_t j = 0; j < tbl->clustered; j++)
        align_column(&tbl->col[j], alignment, tbl->length);

    for (size_t j = tbl->clustered + 1; j < tbl->col_count; j++)
        align_column(&tbl->col[j], alignment, tbl->length);

    free(alignment);
    cs165_log(stderr, "clustered sorting, %d\n", tbl->clustered);
}

static
struct status bulk_load(struct table *tbl, char *rawdata) {
    struct status st;

    int row[tbl->col_count];
    char *nl = "\n";
    char *comma = ",";
    char *brkl, *brkn;
    for (char *line = strtok_r(rawdata, nl, &brkl); line;
               line = strtok_r(NULL, nl, &brkl)) {
            int i = 0;
            for (char *num = strtok_r(line, comma, &brkn); num;
                       num = strtok_r(NULL, comma, &brkn)) {
                row[i++] = strtol(num, NULL, 10);
            }
            bulk_rel_insert(tbl, row);
    }

    if (tbl->clustered < tbl->col_count) mk_cluster(tbl);

    //cs165_log(stderr, "bulk load complete, %d\n", tbl->length);
    return st;
}

//////////////////////////////////////////////////////////////////////////////

// TODO: interpolation search

static
int index_of_left(int *a, int left_range, int length) {
    if (a[length-1] < left_range) return -1;

    int low = 0;
    int high = length-1;

    while (low <= high) {
        int mid = low + ((high - low) / 2);

        if (a[mid] >= left_range)
            high = mid - 1;
        else //if(a[mid]<i)
            low = mid + 1;
    }

    return high + 1;
}

static
int index_of_right(int *a, int right_range, int length) {
    if (a[0] > right_range) {
        cs165_log(stderr, "\t%d, %d\n", a[0], right_range);
        return -1;
    }

    int low = 0;
    int high = length - 1;

    while (low <= high) {
        int mid = low + ((high - low) / 2);

        if (a[mid] > right_range)
            high = mid - 1;
        else //if(a[mid]<i)
            low = mid + 1;
    }

    return low - 1;
}

static inline
size_t scan_clustered(int **v, int low, int high, size_t sz, int *vals) {
    int low_idx = index_of_left(vals, low, sz);
    int high_idx = index_of_right(vals, high, sz);
    //bool flag = low_idx == -1 || high_idx == -1 || low_idx > high_idx;

    size_t num_tuples = low_idx > high_idx ? 0 : high_idx - low_idx + 1;
    cs165_log(stderr, "scan_clustered: %d %d %zu\n", low_idx, high_idx, num_tuples);
    int *vec = malloc(num_tuples * sizeof *vec);
    for (size_t j = 0; j < num_tuples; j++)
        vec[j] = low_idx + j;
    *v = vec;
    return num_tuples;
}

static inline
size_t scan_unsorted(int **v, int low, int high, size_t sz, int *vals) {
    size_t num_tuples = 0;
    int *vec = malloc(sizeof(int) * sz);
    for (size_t j = 0; j < sz; j++) {
        vec[num_tuples] = j;
        num_tuples += (vals[j] >= low && vals[j] < high);
        /* if (data->vals[j] >= low && data->vals[j] < high)
            vec[num_tuples++] = j; */
    }
    cs165_log(stderr, "scan_unsorted: %zu\n", num_tuples);
    *v = realloc(vec, sizeof(int) * num_tuples);
    return num_tuples;
}

static
struct status col_scan(int low, int high, struct column *col, struct cvec **r) {
    status st;
    struct vec const *data = &col->data;
    struct vec *btree_result = NULL;
    size_t num_tuples = 0;
    int *vec = NULL;
    if (col->index) {
        switch (col->index->type) {
            case SORTED:
                num_tuples = sindex_scan(&vec, low, high, data->sz, col->index->index);
                break;
            case BTREE:
                btree_result = btree_rsearch(col->index->index, low, high);
                num_tuples = btree_result->sz;
                vec = btree_result->vals;
                break;
            case IDX_INVALID: assert(false);
        }
    } else if (col->clustered) {
        num_tuples = scan_clustered(&vec, low, high, data->sz, data->vals);
    } else {
        num_tuples = scan_unsorted(&vec, low, high, data->sz, data->vals);
    }

    //cs165_log(stdout, "select: sz %d, low %d, high %d, num_tuples %d\n", data->sz, low, high, num_tuples);
    struct cvec *ret = malloc(sizeof(struct cvec));
    ret->num_tuples = num_tuples;
    ret->values = vec;
    ret->type = VECTOR;
    *r = ret;

    return st;
}

static
struct status vec_scan(int low, int high, struct cvec *pos, struct cvec *vals, struct cvec **r) {
    status st;

    size_t const length = pos->num_tuples;
    size_t num_tuples = 0;
    int *vec = malloc(sizeof(int) * length);
    for (size_t j = 0; j < length; j++) {
        vec[num_tuples] = pos->values[j];
        num_tuples += (vals->values[j] >= low && vals->values[j] < high);
    }

    struct cvec *ret = malloc(sizeof(struct cvec));
    ret->num_tuples = num_tuples;
    ret->values = realloc(vec, sizeof(int) * num_tuples);;
    ret->type = VECTOR;
    *r = ret;

    return st;
}

    //cs165_log(stdout, "select: sz %d, low %d, high %d, num_tuples %d\n", length, low, high, num_tuples);
//////////////////////////////////////////////////////////////////////////////

static status col_fetch(struct column *col, struct cvec *v, struct cvec **r) {
    status st;

    int *resv = malloc(sizeof(int) * v->num_tuples);
    assert(resv);
    for (size_t j = 0; j < v->num_tuples; j++)
        resv[j] = col->data.vals[v->values[j]];

    struct cvec *ret = malloc(sizeof(struct cvec));
    cs165_log(stdout, "num_tuples in fetch: %d\n", v->num_tuples);
    ret->num_tuples = v->num_tuples;
    ret->values = resv;
    ret->type = VECTOR;
    *r = ret;

    return st;
}

static
struct status reconstruct(struct cvec *vecs, struct cvec **r) {
    status st;
    *r = vecs;
    return st;
}

static
struct cvec *find_min(int *vals, size_t sz, bool sorted) {
    struct cvec *min = malloc(sizeof(struct cvec));
    assert(min);

    int m = vals[0];
    if (!sorted)
        for (size_t j = 1; j < sz; j++)
            m = (vals[j] < m) ? vals[j] : m;

    min->ival = m;
    min->num_tuples = 1;
    min->type = INT_VAL;
    return min;
}

static
struct cvec *find_max(int *vals, size_t sz, bool sorted) {
    struct cvec *max = malloc(sizeof(struct cvec));
    assert(max);

    int m = vals[sz-1];
    if (!sorted)
        for (size_t j = 0; j < sz-1; j++)
            m = (vals[j] > m) ? vals[j] : m;

    max->ival = m;
    max->num_tuples = 1;
    max->type = INT_VAL;
    return max;
}

static
struct cvec *find_avg(int *vals, size_t sz) {
    struct cvec *avg = malloc(sizeof(struct cvec));
    assert(avg);

    long int sum = 0;
    for (size_t j = 0; j < sz; j++) sum += (long int) vals[j];
    avg->dval = (long double) sum / sz;

    fprintf(stderr, "avg = %Lf\n", avg->dval);
    //cs165_log(stderr, "average = %Lf\n", avg->dval);
    avg->num_tuples = 1;
    avg->type = DOUBLE_VAL;
    return avg;
}

static
struct cvec *aggregate_max(struct column *c) {
    if (c->clustered)
        return find_max(c->data.vals, c->data.sz, true);

    /*
    if (c->index) {
        switch (c->index->type) {
            // TODO change to sindex fmin
            case SORTED: return NULL;
            // TODO change to btree function
            case BTREE: return NULL;
            case IDX_INVALID: assert(false);
        }
    }
    */
    return find_max(c->data.vals, c->data.sz, false);
}

static
struct cvec *aggregate_min(struct column *c) {
    if (c->clustered)
        return find_min(c->data.vals, c->data.sz, true);

    /*
    if (c->index) {
        switch (c->index->type) {
            // TODO change to sindex fmin
            case SORTED: return NULL;
            // TODO change to btree function
            case BTREE: return NULL;
            case IDX_INVALID: assert(false);
        }
    }
    */
    return find_min(c->data.vals, c->data.sz, false);
}

static
struct status aggregate_col(struct column *c, enum aggr agg, struct cvec **r) {
    status st;
    switch(agg) {
        case MIN: *r = aggregate_min(c); break;
        case MAX: *r = aggregate_max(c); break;
        case AVG: *r = find_avg(c->data.vals, c->data.sz); break;
    }
    return st;
}

static
struct status aggregate_res(struct cvec *vals, enum aggr agg, struct cvec **r) {
    status st;
    switch(agg) {
        case MIN:
            *r = find_min(vals->values, vals->num_tuples, false);
            break;
        case MAX:
            *r = find_max(vals->values, vals->num_tuples, false);
            break;
        case AVG:
            *r = find_avg(vals->values, vals->num_tuples);
            break;
    }
    return st;
}

static
struct status add_vecs(struct cvec *vals1, struct cvec *vals2, struct cvec **r) {
    status st;

    size_t num_tuples = vals1->num_tuples;
    long int *addv = malloc(num_tuples * sizeof *addv);
    for (size_t j = 0; j < num_tuples; j++)
        addv[j] = (long int) vals1->values[j] + (long int) vals2->values[j];

    struct cvec *ret = malloc(sizeof(struct cvec));
    ret->num_tuples = num_tuples;
    ret->long_values = addv;
    ret->type = LONG_VECTOR;
    *r = ret;

    return st;
}

static
struct status sub_vecs(struct cvec *vals1, struct cvec *vals2, struct cvec **r) {
    status st;

    size_t num_tuples = vals1->num_tuples;
    long int *addv = malloc(sizeof *addv * num_tuples);
    for (size_t j = 0; j < num_tuples; j++)
        addv[j] = (long int) vals1->values[j] - (long int) vals2->values[j];

    struct cvec *ret = malloc(sizeof(struct cvec));
    ret->num_tuples = num_tuples;
    ret->long_values = addv;
    ret->type = LONG_VECTOR;
    *r = ret;

    return st;
}

/** execute_db_operator takes as input the db_operator and executes the query.
 * It should return the result (currently as a char*, although I'm not clear
 * on what the return type should be, maybe a result struct, and then have
 * a serialization into a string message).
 **/
//status query_execute(db_operator* op, result** results);
struct status execute_db_operator(db_operator *query, struct cvec **r) {
    struct status st = { ERROR, "query is NULL" };
    if (query == NULL) return st;
    bool insert = true;
    switch(query->type) {
        case(CREATE):
            st = create(query);
            insert = false;
            break;
        case(BULK_LOAD):
            st = bulk_load(query->tables, query->rawdata);
            insert = false;
            break;
        case(INSERT):
            st = rel_insert(query->tables, query->value1);
            insert = false;
            break;
        case(TUPLE):
            st = reconstruct(query->vals1, r);
            insert = false;
            break;
        case(SELECT):
            st = col_scan(query->range.low, query->range.high, query->columns, r);
            break;
        case(PROJECT):
            st = col_fetch(query->columns, query->pos1, r);
            break;
        case(SELECT2):
            st = vec_scan(query->range.low, query->range.high, query->pos1, query->vals1, r);
            break;
        case(AGGREGATE_COL):
            st = aggregate_col(query->columns, query->agg, r);
            break;
        case(AGGREGATE_RES):
            st = aggregate_res(query->vals1, query->agg, r);
            break;
        case(ADD):
            st = add_vecs(query->vals1, query->vals2, r);
            break;
        case(SUB):
            st = sub_vecs(query->vals1, query->vals2, r);
            break;

        case(JOIN): break;
        case(HASH_JOIN): break;
        case(DELETE): break;
        case(UPDATE): break;

        case(MERGE_JOIN): break;
        default: break;
    }

    insert && map_insert(query->assign_var, *r, RESULT);
    free(query);
    return st;
}

