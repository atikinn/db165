#include <string.h>
#include <assert.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>

#include "parser.h"
#include "symtable.h"
#include "dbo.h"
#include "message.h"

static status create_db(const char *db_name, db **db) {
    *db = malloc(sizeof **db);
    if (*db == NULL) {
        status s = { ERROR, "out of memory" };
        return s;
    }
    (*db)->name = db_name;
    (*db)->table_count = 0;
    // TODO: add the logic for the dynamic array; very unlikely to grow
    (*db)->tables = malloc(sizeof(struct table) * 8);
    (*db)->capacity = 8;
    status s = { OK, NULL };
    return s;
}

static status create_table(db* db, const char* name, size_t num_cols, table** table) {
    *table = malloc(sizeof **table);
    if (*table == NULL) {
        status s = { ERROR, "out of memory" };
        return s;
    }
    (*table)->name = strdup(name);
    (*table)->col_count = num_cols;
    (*table)->col = malloc(sizeof(struct column) * num_cols);
    assert((*table)->col);
    (*table)->length = 0;

    // TODO: add the logic for the dynamic array; very unlikely to grow
    db->tables[db->table_count] = **table;
    db->table_count++;

    status s = { OK, NULL };
    return s;
}

static status create_column(table *table, const char* name, column **col) {
    *col = malloc(sizeof **col);
    if (*col == NULL) {
        status s = { ERROR, "out of memory" };
        return s;
    }
    (*col)->name = name;
    (*col)->data = NULL;
    (*col)->index = NULL;
    (*col)->table = table;
    table->col[table->col_count] = **col;
    table->col_count++;

    status s = { OK, NULL };
    return s;
}

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

static status create(db_operator *query) {
    status st;
    db *db;
    table *tbl;
    column *col;
    bool ret;
    switch(query->create_type) {
        case(CREATE_DB):
            st = create_db(query->create_name, &db);
            db_set_current(db);
            break;
        case(CREATE_TBL):
            st = create_table(query->db, query->create_name, query->table_size, &tbl);
            ret = map_insert(query->assign_var, tbl);
            break;
        case(CREATE_COL):
            st = create_column(query->tables, query->create_name, &col);
            ret = map_insert(query->assign_var, col);
            break;
        case(CREATE_IDX): break;
        default: break;
    }
    return st;
}

static status rel_insert(table *tbl, int *row, result **r) {
    status st;

    int fd;
    size_t length = tbl->length;
    size_t filesize = length * sizeof(int);
    off_t lastpage_offset = ((length * 4) / 4096) * 4096;
    size_t num_cols = tbl->col_count;

    for (size_t i = 0; i < num_cols; i++) {
        column *col = &tbl->col[i];
        fd = open(col->name, O_RDWR);
        col->data = mmap(NULL, filesize, PROT_READ|PROT_WRITE,
                         MAP_FILE|MAP_SHARED|MAP_NOCACHE, fd, lastpage_offset);
        close(fd);
        col->data[length - lastpage_offset * 4096] = row[i];
        munmap(col->data, filesize);
    }

    *r = malloc(sizeof(struct result));
    (*r)->num_tuples = -1;
    (*r)->payload = 0; //TODO need to chage to void* to send msg back
    return st;
}

/** execute_db_operator takes as input the db_operator and executes the query.
 * It should return the result (currently as a char*, although I'm not clear
 * on what the return type should be, maybe a result struct, and then have
 * a serialization into a string message).
 **/
//status query_execute(db_operator* op, result** results);
result *execute_db_operator(db_operator *query) {
    status st;
    result *r = NULL;
    bool ret;
    switch(query->type) {
        case(CREATE):
            st = create(query);
            break;
        case(SELECT):
            st = col_scan(query->range.low, query->range.high, query->columns, &r);
            ret = map_insert(query->assign_var, r);
            break;
        case(PROJECT):
            st = col_fetch(query->columns, query->pos1, &r);
            ret = map_insert(query->assign_var, r);
            break;
        case(INSERT):
            st = rel_insert(query->tables, query->value1, &r);
            ret = map_insert(query->assign_var, r);
            break;
        case(TUPLE):
            break;
        case(BULK_LOAD):
            break;
        case(SYNC):
            //TODO sync everything
            break;
        case(AGGREGATE):
            break;

        case(HASH_JOIN): break;
        case(MERGE_JOIN): break;
        case(DELETE): break;
        case(UPDATE): break;
        default: break;
    }

    free(query);
    return r;
}

