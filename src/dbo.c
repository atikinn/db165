#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>
#include <assert.h>
#include <stddef.h>

#include "dbo.h"
#include "symtable.h"
#include "parse.h"
#include "utils.h"
#include "vector.h"

static const char *DB = "db";
static const char *TBL = "tbl";
static const char *COL = "col";
static const char *IDX = "idx";

#define VARNAME_SIZE 16

static db *curdb;

static db *select_db(const char *db_name) {
    (void)db_name;
    //TODO: get the db from vector of db
    return curdb;
}

struct db *get_curdb() {
    return curdb;
}

bool db_set_current(db *db) {
    if (db == NULL) return false;
    curdb = db;
    return true;
}

/** Operator functions **/
static inline char *trim_quotes_copy(const char *name) {
    size_t len = strlen(name);
    char buf[len-1];
    buf[len-2] = '\0';
    strncpy(buf, name + 1, len-2);
    return strdup(buf);
}

static db_operator *cmd_create_db(const char *db_name) {
    db_operator *dbo = malloc(sizeof *dbo);
    if (dbo == NULL) return NULL;
    dbo->create_name = trim_quotes_copy(db_name);
    dbo->type = CREATE;
    dbo->create_type = CREATE_DB;
    return dbo;
}

static db_operator *cmd_create_tbl(const char *tbl_name, const char *db_name, size_t size) {
    db_operator *dbo = malloc(sizeof *dbo);
    if (dbo == NULL) return NULL;
    dbo->create_name = trim_quotes_copy(tbl_name);
    dbo->db = select_db(db_name);
    dbo->table_size = size;
    dbo->type = CREATE;
    dbo->create_type = CREATE_TBL;
    char varbuf[VARNAME_SIZE];
    snprintf(varbuf, sizeof varbuf, "%s.%s", db_name, dbo->create_name);
    dbo->assign_var = strdup(varbuf);
    cs165_log(stderr, "create_tbl varname = %s\n", dbo->assign_var);
    return dbo;
}

static bool should_be_sorted(const char *str) {
    if (!strcmp(str, "sorted")) return true;
    if (!strcmp(str, "unsorted")) return false;
    return false;
}

static
db_operator *cmd_create_column(const char *col_name, const char *db_tbl_name,
                                                     const char *sorted) {
    db_operator *dbo = malloc(sizeof *dbo);
    if (dbo == NULL) return NULL;

    dbo->create_name = trim_quotes_copy(col_name);

    dbo->sorted = should_be_sorted(sorted);

    dbo->tables = map_get(db_tbl_name);
    assert(dbo->tables);

    dbo->type = CREATE;
    dbo->create_type = CREATE_COL;

    char varbuf[VARNAME_SIZE];
    snprintf(varbuf, sizeof varbuf, "%s.%s", dbo->tables->name, dbo->create_name);
    dbo->assign_var = strdup(varbuf);
    cs165_log(stderr, "create_col varname = %s\n", dbo->assign_var);

    return dbo;
}

static enum create get_create_type(const char *type) {
    if (!strcmp(type, DB)) return CREATE_DB;
    if (!strcmp(type, TBL)) return CREATE_TBL;
    if (!strcmp(type, COL)) return CREATE_COL;
    if (!strcmp(type, IDX)) return CREATE_IDX;
    return CREATE_INVALID;
}

db_operator *cmd_create(size_t argc, const char **argv) {
    (void)argc;
    switch(get_create_type(argv[0])) {
        case CREATE_DB:
            return cmd_create_db(argv[1]);
        case CREATE_TBL:
            return cmd_create_tbl(argv[1], argv[2], strtol(argv[3], NULL, 10));
        case CREATE_COL:
            return cmd_create_column(argv[1], argv[2], argv[3]);
        case CREATE_IDX: break;
        default: break;
    }
    return NULL;
}

db_operator *cmd_rel_insert(size_t argc, const char **argv) {
    db_operator *dbo = malloc(sizeof *dbo);
    if (dbo == NULL) return NULL;
    dbo->type = INSERT;
    dbo->value1 = malloc(sizeof(int) * argc-1);
    assert(dbo->value1);
    dbo->tables = map_get(argv[0]);
    for (size_t i = 0; i < argc-1; i++)
        dbo->value1[i] = strtol(argv[i+1], NULL, 10);
    return dbo;
}


db_operator *cmd_select(size_t argc, const char **argv) {
    db_operator *dbo = malloc(sizeof *dbo);
    if (dbo == NULL) return NULL;
    dbo->columns = map_get(argv[argc-3]);
    dbo->range.low = strtol(argv[argc-2], NULL, 10);
    dbo->range.high = strtol(argv[argc-1], NULL, 10);
    dbo->assign_var = strdup(argv[0]);
    dbo->type = SELECT;
    if (argc == 5) dbo->pos1 = map_get(argv[1]);
    return dbo;
}

db_operator *cmd_fetch(size_t argc, const char **argv) {
    (void)argc;
    db_operator *dbo = malloc(sizeof *dbo);
    if (dbo == NULL) return NULL;
    dbo->assign_var = strdup(argv[0]);
    dbo->columns = map_get(argv[1]);
    dbo->pos1 = map_get(argv[2]);
    dbo->type = PROJECT;
    return NULL;
}

db_operator *cmd_tuple(size_t argc, const char **argv) {
    (void)argc;
    db_operator *dbo = malloc(sizeof *dbo);
    if (dbo == NULL) return NULL;
    dbo->vecs = malloc(sizeof(void *) * (argc + 1));
    size_t i = 0;
    for (; i < argc; i++)
        dbo->vecs[i] = map_get(argv[i]);
    dbo->vecs[i] = NULL;
    return dbo;
}

/* not used at the moment */
/*
static
struct column *get_columns(char *rawdata, size_t sz) {
    char header[sz+1];
    header[sz] = '\0';
    strncpy(header, rawdata, sz);
    int num_cols = count_ch(header, ',');
    struct column *cols = malloc(num_cols * sizeof *cols);
    assert(cols);

    int i = 0;
    for (char *tmp, *arg = strtok_r(header, &COMMA, &tmp); arg;
        arg = strtok_r(NULL, &COMMA, &tmp)) {
        cs165_log(stderr, "%s\n", arg);
        struct column *c = map_get(arg);
        if (c != NULL) cols[i++] = *c;
    }

    return cols;
}
*/
db_operator *cmd_load(char *rawdata) {
    db_operator *dbo = malloc(sizeof *dbo);
    if (dbo == NULL) return NULL;
    dbo->type = BULK_LOAD;

    dbo->rawdata = strchr(rawdata, '\n') + 1;
    char *tbl_name_end = strchr(rawdata, COMMA);
    ptrdiff_t len = tbl_name_end - rawdata;
    char tbl_name[len + 1];
    strncpy(tbl_name, rawdata, len);
    char *col_dot = strrchr(tbl_name, '.');
    *col_dot = '\0';

    cs165_log(stderr, "%s\n", tbl_name);
    dbo->tables = map_get(tbl_name);
    return dbo;
}


/** stubs **/
db_operator *cmd_avg(size_t argc, const char **argv) {
    (void)argc;
    (void)argv;
    return NULL;
}

db_operator *cmd_delete(size_t argc, const char **argv) {
    (void)argc;
    (void)argv;
    return NULL;
}

db_operator *cmd_min(size_t argc, const char **argv) {
    (void)argc;
    (void)argv;
    return NULL;
}

db_operator *cmd_max(size_t argc, const char **argv) {
    (void)argc;
    (void)argv;
    return NULL;
}

db_operator *cmd_hashjoin(size_t argc, const char **argv) {
    (void)argc;
    (void)argv;
    return NULL;
}
db_operator *cmd_mergejoin(size_t argc, const char **argv) {
    (void)argc;
    (void)argv;
    return NULL;
}
db_operator *cmd_rel_delete(size_t argc, const char **argv) {
    (void)argc;
    (void)argv;
    return NULL;
}
db_operator *cmd_sub(size_t argc, const char **argv) {
    (void)argc;
    (void)argv;
    return NULL;
}
db_operator *cmd_update(size_t argc, const char **argv) {
    (void)argc;
    (void)argv;
    return NULL;
}
db_operator *cmd_add(size_t argc, const char **argv) {
    (void)argc;
    (void)argv;
    return NULL;
}

