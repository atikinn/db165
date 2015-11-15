#include <stdio.h>
#include <string.h>
#include <stdbool.h>
#include <assert.h>

#include "dbo.h"
#include "symtable.h"

static const char *DB = "db";
static const char *TBL = "tbl";
static const char *COL = "col";
static const char *IDX = "idx";

static db *curdb;

static db *select_db(const char *db_name) {
    (void)db_name;
    //TODO: get the db from vector of db
    return curdb;
}

void db_set_current(db *db) {
    curdb = db;
    return;
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

static db_operator *cmd_create_tbl(const char *tbl_name, const char *db_name, int size) {
    db_operator *dbo = malloc(sizeof *dbo);
    if (dbo == NULL) return NULL;
    dbo->create_name = trim_quotes_copy(tbl_name);
    dbo->db = select_db(db_name);
    dbo->table_size = size;
    dbo->type = CREATE;
    dbo->create_type = CREATE_TBL;
    return dbo;
}

static bool should_be_sorted(const char *str) {
    if (!strcmp(str, "sorted")) return true;
    if (!strcmp(str, "unsorted")) return false;
    return false;
}

static db_operator *cmd_create_column(const char *col_name, table *tbl, const char *sorted) {
    db_operator *dbo = malloc(sizeof *dbo);
    if (dbo == NULL) return NULL;
    dbo->create_name = trim_quotes_copy(col_name);
    dbo->sorted = should_be_sorted(sorted);
    dbo->tables = tbl;
    dbo->type = CREATE;
    dbo->create_type = CREATE_TBL;
    return dbo;
}

static enum create get_create_type(const char *type) {
    if (!strcmp(type, DB)) return CREATE_DB;
    if (!strcmp(type, TBL)) return CREATE_TBL;
    if (!strcmp(type, COL)) return CREATE_COL;
    if (!strcmp(type, IDX)) return CREATE_IDX;
    return CREATE_INVALID;
}

db_operator *cmd_create(int argc, const char **argv) {
    (void)argc;
    table *tbl;
    switch(get_create_type(argv[0])) {
        case CREATE_DB:
            return cmd_create_db(argv[1]);
        case CREATE_TBL:
            return cmd_create_tbl(argv[1], argv[2], strtol(argv[3], NULL, 10));
        case CREATE_COL:
            tbl = map_get(strchr(argv[2], '.') + 1);
            return cmd_create_column(argv[1], tbl, argv[3]);
        case CREATE_IDX: break;
        default: break;
    }
    return NULL;
}

db_operator *cmd_rel_insert(int argc, const char **argv) {
    db_operator *dbo = malloc(sizeof *dbo);
    if (dbo == NULL) return NULL;
    dbo->type = INSERT;
    dbo->value1 = malloc(sizeof(int) * argc-1);
    dbo->tables = map_get(argv[0]);
    assert(dbo->value1);
    for (int i = 1; i < argc; i++)
        dbo->value1[i] = strtol(argv[i], NULL, 10);
    return dbo;
}

db_operator *cmd_select(int argc, const char **argv) {
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

db_operator *cmd_fetch(int argc, const char **argv) {
    (void)argc;
    db_operator *dbo = malloc(sizeof *dbo);
    if (dbo == NULL) return NULL;
    dbo->assign_var = strdup(argv[0]);
    dbo->columns = map_get(argv[1]);
    dbo->pos1 = map_get(argv[2]);
    dbo->type = PROJECT;
    return NULL;
}

db_operator *cmd_tuple(int argc, const char **argv) {
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

/** stubs **/
db_operator *cmd_avg(int argc, const char **argv) {
    (void)argc;
    (void)argv;
    return NULL;
}
db_operator *cmd_delete(int argc, const char **argv) {
    (void)argc;
    (void)argv;
    return NULL;
}
db_operator *cmd_min(int argc, const char **argv) {
    (void)argc;
    (void)argv;
    return NULL;
}
db_operator *cmd_max(int argc, const char **argv) {
    (void)argc;
    (void)argv;
    return NULL;
}
db_operator *cmd_load(int argc, const char **argv) {
    (void)argc;
    (void)argv;
    return NULL;
}
db_operator *cmd_sync(int argc, const char **argv) {
    (void)argc;
    (void)argv;
    return NULL;
}
db_operator *cmd_hashjoin(int argc, const char **argv) {
    (void)argc;
    (void)argv;
    return NULL;
}
db_operator *cmd_mergejoin(int argc, const char **argv) {
    (void)argc;
    (void)argv;
    return NULL;
}
db_operator *cmd_rel_delete(int argc, const char **argv) {
    (void)argc;
    (void)argv;
    return NULL;
}
db_operator *cmd_sub(int argc, const char **argv) {
    (void)argc;
    (void)argv;
    return NULL;
}
db_operator *cmd_update(int argc, const char **argv) {
    (void)argc;
    (void)argv;
    return NULL;
}
db_operator *cmd_add(int argc, const char **argv) {
    (void)argc;
    (void)argv;
    return NULL;
}
