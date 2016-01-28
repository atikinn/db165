#include <string.h>
#include <assert.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <limits.h>

#include "parse.h"
#include "symtable.h"
#include "dbo.h"
#include "message.h"
#include "utils.h"
#include "vector.h"
#include "sindex.h"
#include "btree.h"
#include "vector.h"
#include "sync.h"

#define DEFAULT_TABLE_COUNT 8

#define L1CACHE_SIZE (2<<17)

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
struct column *create_column(table *table, char *name, bool sorted) {
    struct column c = { .name = name, .index = NULL, .table = table,
                        .clustered = false, .status = MODIFIED };
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
    // TODO: case where col->index->index == NULL: load after create cmd
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
static inline
void insert_btree(struct btree *bt, int key, int rid, bool clustered) {
    btree_insert(bt, key, rid);
    if (clustered == false)
        btree_increment_cond(bt->iter, key, rid);
}

static
void index_insert(struct column *c, int val, size_t pos, size_t sz) {
    if (c->index == NULL) return;
    switch(c->index->type) {
        case BTREE:
            insert_btree(c->index->index, val, pos, c->clustered);
            return;
        case SORTED:
            c->index->index = sindex_insert(c->index->index, pos, val, sz);
            return;
        case IDX_INVALID: assert(false);
    }
}

static inline
void column_insert(struct column *col, int value, size_t pos) {
    vector_insert(&col->data, value, pos);
    col->status = MODIFIED;
}

static inline
size_t column_sorted_insert(struct column *col, int value) {
    size_t pos = vector_insert_sorted(&col->data, value);
    col->status = MODIFIED;
    return pos;
}

static
status rel_insert(struct table *tbl, int *row) {
    status st;

    if (tbl->clustered < tbl->col_count) {
        size_t pos = column_sorted_insert(&tbl->col[tbl->clustered], row[tbl->clustered]);
        for (unsigned j = 0; j < tbl->col_count; j++) {
            if (j != tbl->clustered) column_insert(&tbl->col[j], row[j], pos);
            index_insert(&tbl->col[j], row[j], pos, tbl->length);
        }
    } else {
        for (unsigned j = 0; j < tbl->col_count; j++) {
            column_insert(&tbl->col[j], row[j], tbl->length);
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

static
void align_col(struct column *col, int *indices, int n) {
    int *a = col->data.vals;
    int i, j, m;

    // permute a and store in indices
    // store inverse permutation in a
     for (j = 0; j < n; ++j) {
         i = indices[j];
         indices[j] = a[i];
         a[i] = j;
     }

     // swap a and indices
     for (j = 0; j < n; ++j) {
         i = indices[j];
         indices[j] = a[j];
         a[j] = i;
     }

     // inverse indices permutation to get the original
     for (i = 0; i < n; ++i)
         indices[i] = -indices[i] - 1;

     for (m = n - 1; m >= 0; --m) {
         // for (i = m, j = indices[m]; j >= 0; i = j, j = indices[j]) ;
         i = m;
         j = indices[m];
         while (j >= 0) {
             i = j;
             j = indices[j];
         }
         indices[i] = indices[-j - 1];
         indices[-j - 1] = m;
    }
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

    (void)align_col;
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
                cs165_log(stderr, "btree_scan: %zu\n", num_tuples);
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
    //TODO LONG_VECTOR
    ret->type = VECTOR;
    *r = ret;

    return st;
}

    //cs165_log(stdout, "select: sz %d, low %d, high %d, num_tuples %d\n", length, low, high, num_tuples);
//////////////////////////////////////////////////////////////////////////////

static
status res_fetch(struct cvec *res, struct cvec *pos, struct cvec **r) {
    status st;

    int *resv = malloc(sizeof(int) * pos->num_tuples);
    assert(resv);

    for (size_t j = 0; j < pos->num_tuples; j++)
        resv[j] = res->values[pos->values[j]];

    struct cvec *ret = malloc(sizeof(struct cvec));
    cs165_log(stdout, "num_tuples in fetch: %d\n", pos->num_tuples);

    ret->num_tuples = pos->num_tuples;
    ret->values = resv;
    ret->type = VECTOR;
    *r = ret;

    return st;
}

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

//TODO reconstruct more than 1 result: design!!!
static
struct status reconstruct(struct cvec *vecs, struct cvec **r) {
    status st;
    *r = vecs;
    return st;
}

//////////////////////////////////////////////////////////////////////////////

static
int find_max(int *vals, size_t sz, bool sorted) {
    int max = vals[sz-1];
    if (!sorted) {
        for (size_t j = 0; j < sz-1; j++) {
            int v = vals[j];
            max = (v > max) ? v : max;
        }
    }
    cs165_log(stderr, "max: %d\n", max);
    return max;
}

static
int find_min(int *vals, size_t sz, bool sorted) {
    cs165_log(stderr, "called find_min\n");

    int min = vals[0];
    if (!sorted)
        for (size_t j = 1; j < sz; j++) {
            int v = vals[j];
            min = (v < min) ? v : min;
        }
    return min;
}

static
long double find_avg(int *vals, size_t sz) {
    long double avg = 0.0;

    long int sum = 0;
    for (size_t j = 0; j < sz; j++)
        sum += vals[j];

    avg = (long double) sum / sz;

    fprintf(stderr, "avg = %Lf\n", avg);
    //cs165_log(stderr, "average = %Lf\n", avg->dval);
    return avg;
}

//////////////////////////////////////////////////////////////////////////////

static
long double find_avg_res(void *vals, size_t sz, enum result_type type) {
    if (type == VECTOR)
        return find_avg(vals, sz);
    assert(type == LONG_VECTOR);

    long int *values = (long int *)vals;
    long double avg = 0.0;

    long int sum = 0;
    for (size_t j = 0; j < sz; j++)
        sum += values[j];

    avg = (long double) sum / sz;

    fprintf(stderr, "avg = %Lf\n", avg);
    //cs165_log(stderr, "average = %Lf\n", avg->dval);
    return avg;
}

static
long int find_max_res(void *vals, size_t sz, bool sorted, enum result_type type) {
    if (type == VECTOR)
        return find_max(vals, sz, sorted);
    assert(type == LONG_VECTOR);

    long int *values = (long int *)vals;
    long int max = values[sz-1];
    if (!sorted) {
        for (size_t j = 0; j < sz-1; j++) {
            long int v = values[j];
            if (v >= 2019184347) cs165_log(stderr, "\t%ld\n", v);
            max = (v > max) ? v : max;
        }
    }
    cs165_log(stderr, "max: %d\n", max);
    return max;
}

static
long int find_min_res(void *vals, size_t sz, bool sorted, enum result_type type) {
    cs165_log(stderr, "called find_min_res\n");
    if (type == VECTOR)
        return find_min(vals, sz, sorted);
    assert(type == LONG_VECTOR);
    long int *values = (long int *)vals;
    long int min = values[0];
    if (!sorted)
        for (size_t j = 1; j < sz; j++) {
            long int v = values[j];
            min = (v < min) ? v : min;
        }
    return min;
}

static
struct status aggregate_res(struct cvec *vals, enum aggr agg, struct cvec **r) {
    cs165_log(stderr, "called aggregate_res\n");
    status st;

    struct cvec *val = malloc(sizeof(struct cvec));
    assert(val);
    val->num_tuples = 1;

    switch(agg) {
        case MIN:
            val->ival = find_min_res(vals->values, vals->num_tuples, false, vals->type);
            val->type = LONG_VAL;
            break;
        case MAX:
            val->ival = find_max_res(vals->values, vals->num_tuples, false, vals->type);
            val->type = LONG_VAL;
            break;
        case AVG:
            val->dval = find_avg_res(vals->values, vals->num_tuples, vals->type);
            val->type = DOUBLE_VAL;
            break;
    }
    *r = val;
    return st;
}
//////////////////////////////////////////////////////////////////////////////

static
int aggregate_max(struct column *c) {
    if (c->clustered)
        return find_max(c->data.vals, c->data.sz, true);

    if (c->index)
        switch (c->index->type) {
            // TODO change to sindex fmin
            case SORTED: break;
            // TODO change to btree function
            case BTREE: break;
            case IDX_INVALID: assert(false);
        }

    return find_max(c->data.vals, c->data.sz, false);
}

static
int aggregate_min(struct column *c) {
    if (c->clustered)
        return find_min(c->data.vals, c->data.sz, true);

    if (c->index) {
        switch (c->index->type) {
            // TODO change to sindex fmin
            case SORTED: break;
            // TODO change to btree function
            case BTREE: break;
            case IDX_INVALID: assert(false);
        }
    }

    return find_min(c->data.vals, c->data.sz, false);
}
static
struct status aggregate_col(struct column *c, enum aggr agg, struct cvec **r) {
    status st;

    struct cvec *val = malloc(sizeof(struct cvec));
    assert(val);
    val->num_tuples = 1;

    switch(agg) {
        case MIN:
            val->ival = aggregate_min(c);
            val->type = LONG_VAL;
            break;
        case MAX:
            val->ival = aggregate_max(c);
            val->type = LONG_VAL;
            break;
        case AVG:
            val->ival = find_avg(c->data.vals, c->data.sz);
            val->type = DOUBLE_VAL;
            break;
    }

    *r = val;
    return st;
}

//////////////////////////////////////////////////////////////////////////////

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

static
struct status nl_join(struct cvec *vals1, struct cvec *pos1,
                      struct cvec *vals2, struct cvec *pos2,
                      struct cvec **rl, struct cvec **rr) {
    status st;

    struct cvec *lres = vals1->num_tuples < vals2->num_tuples ? vals1 : vals2;
    int *lpos = pos1->num_tuples < pos2->num_tuples ? pos1->values : pos2->values;
    int *lmatch = malloc(sizeof(int) * lres->num_tuples);
    assert(lmatch);

    struct cvec *rres = vals1->num_tuples < vals2->num_tuples ? vals2 : vals1;
    int *rpos = pos1->num_tuples < pos2->num_tuples ? pos2->values : pos1->values;
    int *rmatch = malloc(sizeof(int) * rres->num_tuples);
    assert(rmatch);

    int blsz = (L1CACHE_SIZE >> 2) / sizeof(int);
    int lcount = 0;
    int rcount = 0;

    for (size_t rbl = 0; rbl < rres->num_tuples; rbl += blsz)
        for (size_t lbl = 0; lbl < lres->num_tuples; lbl += blsz) {
            size_t rlim = rbl + blsz;
            size_t llim = lbl + blsz;
            for (size_t r = rbl; r < rlim && r < rres->num_tuples; r++)
                for (size_t l = lbl; l < llim && l < lres->num_tuples; l++)
                    if (lres->values[l] == rres->values[r]) {
                        lmatch[lcount++] = lpos[l];
                        rmatch[rcount++] = rpos[r];
                    }
        }

    struct cvec *res1 = malloc(sizeof *res1);
    res1->values = realloc(lmatch, sizeof(int) * lcount);
    res1->num_tuples = lcount;
    res1->type = VECTOR;
    *rl = res1;

    struct cvec *res2 = malloc(sizeof *res2);
    res2->values = realloc(rmatch, sizeof(int) * rcount);
    res2->num_tuples = rcount;
    res2->type = VECTOR;
    *rr = res2;

    cs165_log(stdout, "num_tuples in nl_join: %d and %d %d\n", lcount, rcount, blsz);
    return st;
}

static
void load_columns(db_operator *q) {
    switch(q->type) {
        case(INSERT):
            for (size_t j = 0; j < q->tables->col_count; j++)
                load_column(&q->tables->col[j]);
            break;
        case(SELECT) : case(PROJECT) : case(AGGREGATE_COL):
            load_column(q->columns);
            break;
        default: break;
    }
}

/** execute_db_operator takes as input the db_operator and executes the query.
 * It should return the result (currently as a char*, although I'm not clear
 * on what the return type should be, maybe a result struct, and then have
 * a serialization into a string message).
 **/
struct status execute_db_operator(db_operator *query, struct cvec **result) {
    struct status st = { ERROR, "query is NULL" };
    if (query == NULL) return st;

    struct cvec *r = NULL;
    struct cvec *r2 = NULL;
    load_columns(query);

    switch(query->type) {
        case(CREATE):
            st = create(query);
            break;
        case(BULK_LOAD):
            st = bulk_load(query->tables, query->rawdata);
            break;
        case(INSERT):
            st = rel_insert(query->tables, query->value1);
            free(query->value1);
            break;
        case(TUPLE):
            st = reconstruct(query->vals1, result);
            break;
        case(SELECT):
            st = col_scan(query->range.low, query->range.high, query->columns, &r);
            break;
        case(PROJECT):
            st = col_fetch(query->columns, query->pos1, &r);
            break;
        case(PROJECT_RES):
            st = res_fetch(query->vals1, query->pos1, &r);
            break;
        case(SELECT2):
            st = vec_scan(query->range.low, query->range.high, query->pos1, query->vals1, &r);
            break;
        case(AGGREGATE_COL):
            st = aggregate_col(query->columns, query->agg, &r);
            break;
        case(AGGREGATE_RES):
            st = aggregate_res(query->vals1, query->agg, &r);
            break;
        case(ADD):
            st = add_vecs(query->vals1, query->vals2, &r);
            break;
        case(SUB):
            st = sub_vecs(query->vals1, query->vals2, &r);
            break;
        case(JOIN):
            st = nl_join(query->vals1, query->pos1, query->vals2, query->pos2, &r, &r2);
            break;
        case(HASH_JOIN): break;

        case(DELETE): break;
        case(UPDATE): break;

        case(MERGE_JOIN): break;
        default: break;
    }

    if (r != NULL) map_insert(query->assign_var, r, RESULT);
    if (r2 != NULL) map_insert(query->assign_var2, r2, RESULT);

    free(query);
    return st;
}

