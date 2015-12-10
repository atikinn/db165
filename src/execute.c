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

#define DEFAULT_TABLE_COUNT 8

static
unsigned int next_pow2(unsigned int n) {
    n--;
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    n |= n >> 8;
    n |= n >> 16;
    n++;
    return n;
}

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
    struct table t = { .name = name, .length = 0, .clustered = 0, .col_count = 0 };
    t.col = malloc(sizeof(struct column) * max_cols);
    assert(t.col);

    // TODO: add the logic for resizing; max tables are 7 in tests
    db->tables[db->table_count] = t;
    return &db->tables[db->table_count++];;
}

static
struct column *create_column(table *table, char* name, bool sorted) {
    struct column c = { .name = name, .index = NULL, .table = table };
    vector_init(&c.data, 16);
    assert(c.data.vals);
    if (sorted) table->clustered = table->col_count;    // clustered
    table->col[table->col_count] = c;
    return &table->col[table->col_count++];;
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
            break;
        case(CREATE_COL):
            col = create_column(query->tables, query->create_name, query->sorted);
            ret = map_insert(query->assign_var, col, ENTITY);
            st.code = ret ? OK : ERROR;
            st.message = "column created";
            break;
        case(CREATE_IDX): break;
        default: break;
    }
    return st;
}

//////////////////////////////////////////////////////////////////////////////

static inline
void column_insert(struct column *c, int val) {
    // TODO after B-tree get rid of vector here to speed up insertions
    vector_push(&c->data, val);

    if (c->index) {
        ; //TODO
    }
}

static
status rel_insert(struct table *tbl, int *row) {
    // TODO clustered check
    status st;

    for (unsigned j = 0; j < tbl->col_count; j++)
        column_insert(&tbl->col[j], row[j]);
    tbl->length++;

    st.code = OK;
    st.message = "insert completed";
    return st;
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
            rel_insert(tbl, row);
    }

    cs165_log(stderr, "bulk load complete, %d\n", tbl->length);
    return st;
}

//////////////////////////////////////////////////////////////////////////////

static
struct status col_scan(int low, int high, struct column *col, struct cvec **r) {
    status st;

    struct vec *data = &col->data;
    size_t num_tuples = 0;
    int *vec = malloc(sizeof(int) * data->sz);
    for (size_t j = 0; j < data->sz; j++) {
        vec[num_tuples] = j;
        num_tuples += (data->vals[j] >= low && data->vals[j] < high);
    }

    /*
    if (data->vals[j] >= low && data->vals[j] < high)
        vec[num_tuples++] = j;
    */
    cs165_log(stdout, "select: sz %d, low %d, high %d, num_tuples %d\n", data->sz, low, high, num_tuples);
    struct cvec *ret = malloc(sizeof(struct cvec));
    ret->num_tuples = num_tuples;
    ret->values = realloc(vec, sizeof(int) * num_tuples);;
    ret->type = VECTOR;
    *r = ret;

    return st;
}

static
struct status vec_scan(int low, int high, struct cvec *pos, struct cvec *vals, struct cvec **r) {
    status st;

    size_t length = pos->num_tuples;
    size_t num_tuples = 0;
    int *vec = malloc(sizeof(int) * length);
    for (size_t j = 0; j < length; j++) {
        vec[num_tuples] = pos->values[j];
        num_tuples += (vals->values[j] >= low && vals->values[j] < high);
    }

    //cs165_log(stdout, "select: sz %d, low %d, high %d, num_tuples %d\n", length, low, high, num_tuples);
    struct cvec *ret = malloc(sizeof(struct cvec));
    ret->num_tuples = num_tuples;
    ret->values = realloc(vec, sizeof(int) * num_tuples);;
    ret->type = VECTOR;
    *r = ret;

    return st;
}

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
struct cvec *find_min(int *vals, size_t sz) {
    struct cvec *min = malloc(sizeof(struct cvec));
    assert(min);

    int m = vals[0];
    for (size_t j = 0; j < sz; j++)
        m = (vals[j] < m) ? vals[j] : m;

    min->ival = m;
    min->num_tuples = 1;
    min->type = INT_VAL;
    return min;
}

static
struct cvec *find_max(int *vals, size_t sz) {
    struct cvec *max = malloc(sizeof(struct cvec));
    assert(max);

    int m = vals[0];
    for (size_t j = 0; j < sz; j++)
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
struct status aggregate_col(struct column *c, enum aggr agg, struct cvec **r) {
    status st;
    switch(agg) {
        case MIN:
            *r = find_min(c->data.vals, c->data.sz);
            break;
        case MAX:
            *r = find_max(c->data.vals, c->data.sz);
            break;
        case AVG:
            *r = find_avg(c->data.vals, c->data.sz);
            break;
    }
    return st;
}

static
struct status aggregate_res(struct cvec *vals, enum aggr agg, struct cvec **r) {
    status st;
    switch(agg) {
        case MIN:
            *r = find_min(vals->values, vals->num_tuples);
            break;
        case MAX:
            *r = find_max(vals->values, vals->num_tuples);
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
    long int *addv = malloc(sizeof *addv * num_tuples);
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
        case(MERGE_JOIN): break;
        case(DELETE): break;
        case(UPDATE): break;
        default: break;
    }

    insert && map_insert(query->assign_var, *r, RESULT);
    free(query);
    return st;
}

