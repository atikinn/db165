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
    struct column c = { .name = name, .data = NULL, .index = NULL, .table = table };
    c.data = vector_create(16);
    assert(c.data);
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
    vector_push(c->data, val);

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

/*
static status col_scan(int low, int high, column *col, result **r) {
    status st;

    struct stat sb;
    int fd = open(col->name, O_RDONLY);
    fstat(fd, &sb);
    col->data = mmap(NULL, sb.st_size, PROT_READ, MAP_SHARED, fd, (off_t)0);
    close(fd);
    madvise(col->data, sb.st_size, MADV_SEQUENTIAL);

    size_t num_tuples = 0;
    size_t col_len = col->table->length;
    // TODO alloca optimization for sizes <= 1 << 20
    int *vec = malloc(sizeof(int) * col_len);

    for (size_t i = 0; i < col_len; i++)
        if (col->data[i] >= low && col->data[i] < high)
            vec[num_tuples++] = i;

    munmap(col->data, sb.st_size);

    *r = malloc(sizeof(struct result));
    (*r)->num_tuples = num_tuples;
    (*r)->payload = realloc(vec, sizeof(int) * num_tuples);;
    return st;
}

//////////////////////////////////////////////////////////////////////////////

static status col_fetch(column *col, result *ivec, result **r) {
    status st;

    struct stat sb;
    int fd = open(col->name, O_RDONLY);
    fstat(fd, &sb);
    col->data = mmap(NULL, sb.st_size, PROT_READ, MAP_FILE | MAP_PRIVATE, fd, (off_t)0);
    close(fd);
    madvise(col->data, sb.st_size, MADV_RANDOM); //TODO try with MADV_WILLNEED in a loop

    size_t len = ivec->num_tuples;
    int *vec = malloc(sizeof(int) * len);
    size_t num_tuples = 0;
    for (; num_tuples < len; num_tuples++)
        vec[num_tuples] = col->data[ivec->payload[num_tuples]];

    munmap(col->data, sb.st_size);

    *r = malloc(sizeof(struct result));
    (*r)->num_tuples = num_tuples;
    (*r)->payload = realloc(vec, sizeof(int) * num_tuples);
    return st;
}
*/

/** execute_db_operator takes as input the db_operator and executes the query.
 * It should return the result (currently as a char*, although I'm not clear
 * on what the return type should be, maybe a result struct, and then have
 * a serialization into a string message).
 **/
//status query_execute(db_operator* op, result** results);
struct result *execute_db_operator(db_operator *query) {
    struct result *r = NULL;
    if (query == NULL) return r;

    status st;
    switch(query->type) {
        case(CREATE):
            st = create(query);
            break;
        case(BULK_LOAD):
            st = bulk_load(query->tables, query->rawdata);
            break;
        case(INSERT):
            st = rel_insert(query->tables, query->value1);
            break;
        case(SELECT):
            //st = col_scan(query->range.low, query->range.high, query->columns, &r);
            //ret = map_insert(query->assign_var, r);
            break;
        case(PROJECT):
            //st = col_fetch(query->columns, query->pos1, &r);
            //ret = map_insert(query->assign_var, r);
            break;

        case(TUPLE): break;
        case(DELETE): break;
        case(UPDATE): break;

        case(AGGREGATE):
            break;
        case(HASH_JOIN): break;
        case(MERGE_JOIN): break;
        //case(SYNC): break;
        default: break;
    }

    free(query);
    return r;
}

