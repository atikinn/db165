#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>
#include <assert.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stddef.h>

#include "cs165_api.h"
#include "vector.h"
#include "utils.h"
#include "dbo.h"
#include "symtable.h"
#include "sync.h"

static const char *DBPATH = "./db";
static const char *METAFILE = "meta";
static const size_t PATHLEN = 128;
static const size_t VARNAME_SIZE = 64;

enum meta_state { DB_RECORD, TBL_RECORD, COL_RECORD };

struct db_record {
    char tbl_count;
    char name[];
};

struct tbl_record {
    size_t length;
    char col_count;
    char clustered;
    char name[];
};

static inline
off_t filemap(const char *path, char **map, int prot) {
    int fd = open(path, O_RDONLY);
    if (fd == -1) {
        perror("Error opening file for writing");
	return -1;
    }

    struct stat s;
    if (fstat(fd, &s) == -1) {
	perror("Cannot get file size");
	return -1;
    }

    *map = mmap(0, s.st_size, prot, MAP_PRIVATE, fd, 0);
    close(fd);
    if (map == MAP_FAILED) {
	perror("Error mmapping the file");
	return -1;
    }

    return s.st_size;
}

void fileunmap(void *map, size_t sz) {
    if (munmap(map, sz) == -1)
	perror("Error un-mmapping the file");
    return;
}

static inline
int mkfile(size_t sz, const char *path) {
    int fd = open(path, O_RDWR | O_CREAT | O_TRUNC, (mode_t)0600);
    if (fd == -1) {
        perror("Error opening file for writing");
	return -1;
    }

    off_t off = lseek(fd, sz-1, SEEK_SET);
    if (off == -1) {
	close(fd);
	perror("Error calling lseek() to 'stretch' the file");
	return -1;
    }

    ssize_t wbytes = write(fd, "", 1);
    if (wbytes != 1) {
	close(fd);
	perror("Error writing last byte of the file");
	return -1;
    }

    return fd;
}

char *vbsnprintf(char *buf, size_t buflen, const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    size_t n = vsnprintf(buf, buflen, fmt, ap);
    if (n >= buflen) {
        buf = malloc(n + 1);
        assert(buf != NULL);
        va_end(ap);
        va_start(ap, fmt);
        size_t m = vsnprintf(buf, n + 1, fmt, ap);
        assert(n == m);
    }
    va_end(ap);
    return buf;
}

static inline
void persist_data(struct vec *data, const char *path) {
    size_t sz = data->sz * sizeof(int);

    int fd = mkfile(sz, path);
    if (fd == -1) return;

    int *map = mmap(0, sz, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);

    if (map == MAP_FAILED) {
	perror("Error mmapping the file");
	return;
    }

    memcpy(map, (void *)data->vals, data->sz * sizeof(int));

    if (munmap(map, sz) == -1) {
	perror("Error un-mmapping the file");
	return;
    }

    return;
}

static inline
void persist_col(struct column *col, const char *tname) {
    char buf[PATHLEN];
    char *path = vbsnprintf(buf, sizeof buf, "%s/%s.%s.bin", DBPATH, tname, col->name);
    persist_data(&col->data, path);
    if (path != buf) free(path);
    return;
}

static inline
void persist_db(struct db *db) {
    for (size_t i = 0; i < db->table_count; i++) {
        struct table *tbl = &db->tables[i];
        for (size_t j = 0; j < tbl->col_count; j++) {
            struct column *col = &tbl->col[j];
            cs165_log(stderr, "%d = %d\n", tbl->length, col->data.vals[tbl->length-1]);
            persist_col(col, tbl->name) ;
        }
    }
}

static inline
size_t get_col_meta_sz(struct column *col) {
    return strlen(col->name) + 1;
}

static inline
size_t col_meta(struct column *col, char *meta, size_t sz, size_t rec_size) {
    char buf[rec_size];
    buf[rec_size-1] = '|';

    memcpy(buf, col->name, rec_size-1);

    memcpy(meta + sz, buf, rec_size);
    return rec_size;
}

static inline
size_t get_tbl_meta_sz(struct table *tbl) {
    return strlen(tbl->name) + sizeof tbl->length + 2 + 1;
}

static inline
size_t tbl_meta(struct table *tbl, char *meta, size_t sz, size_t rec_size) {
    char buf[rec_size];
    buf[rec_size-1] = '|';

    struct tbl_record *rec = (struct tbl_record *) buf;
    rec->col_count = (unsigned char) tbl->col_count;
    rec->clustered = (unsigned char) tbl->clustered;
    rec->length = tbl->length;
    memcpy(rec->name, tbl->name, strlen(tbl->name));

    memcpy(meta + sz, buf, rec_size);
    return rec_size;
}

static inline
size_t get_db_meta_sz(struct db *db) {
    return strlen(db->name) + 1 + 1;
}

static inline
size_t db_meta(struct db *db, char *meta, size_t sz, size_t rec_size) {
    char buf[rec_size];
    buf[rec_size-1] = '|';

    struct db_record *rec = (struct db_record *) buf;
    rec->tbl_count = (unsigned char) db->table_count;
    memcpy(rec->name, db->name, strlen(db->name));

    memcpy(meta + sz, buf, rec_size);
    return rec_size;
}

static inline
char *resize_meta(char *meta, size_t capacity) {
    char *tmp = realloc(meta, sizeof(char) * capacity * 2);
    assert(tmp);
    return tmp;
}

static inline
size_t mk_metadata(struct db *db, char **ret) {
    size_t capacity = 64, sz = 0;
    char *meta = malloc(capacity);
    assert(meta);

    size_t rec_size = get_db_meta_sz(db);
    if (sz + rec_size >= capacity)
	meta = resize_meta(meta, capacity);
    sz += db_meta(db, meta, sz, rec_size);

    for (size_t i = 0; i < db->table_count; i++) {
        struct table *tbl = &db->tables[i];
	rec_size = get_tbl_meta_sz(tbl);
        if (sz + rec_size >= capacity)
	    meta = resize_meta(meta, capacity);
        sz += tbl_meta(tbl, meta, sz, rec_size);
        for (size_t j = 0; j < tbl->col_count; j++) {
            struct column *col = &tbl->col[j];
	    rec_size = get_col_meta_sz(col);
            if (sz + rec_size >= capacity)
		meta = resize_meta(meta, capacity);
            sz += col_meta(col, meta, sz, rec_size);
        }
    }

    *ret = meta;
    return sz;
}

static inline
void persist_meta(char *meta, size_t sz) {
    char buf[PATHLEN];
    char *path = vbsnprintf(buf, sizeof buf, "%s/%s.bin", DBPATH, METAFILE);
    struct vec v = { sz / 4 + 1, -1, (int *) meta };
    persist_data(&v, path);
    if (path != buf) free(path);
    return;
}

static inline
void persist_metadata(struct db *db) {
    struct stat st;
    if (stat(DBPATH, &st) == -1)
        if (mkdir(DBPATH, (mode_t)0700) == -1)
	    perror("mkdir failed");

    char *meta;
    size_t sz = mk_metadata(db, &meta);
    persist_meta(meta, sz);
    free(meta);
    return;
}

static
void clean_db(struct db *db) {
    for (size_t i = 0; i < db->table_count; i++) {
	struct table *tbl = &db->tables[i];
	for (size_t j = 0; j < tbl->col_count; j++) {
	    struct column *col = &tbl->col[j];
	    free(col->name);
	    free(col->data.vals);
	    if (col->index) free(col->index);
	}
	free(tbl->name);
	free(tbl->col);
    }
    free(db->tables);
    free(db->name);
    free(db);
    return;
}

void sync(void) {
    struct db *db = get_curdb();
    persist_metadata(db);
    persist_db(db);
    clean_db(db);
    clean_symtbl();
}

//////////////////////////////////////////////////////////////////////////////

static
struct column restore_col(char *record, struct table *tbl) {
    struct column col = { .name = strdup(record), .table = tbl, .index = NULL };
    vector_init(&col.data, tbl->length);
    return col;
}

static
struct table restore_tbl(char *record, size_t *col_count) {
    struct tbl_record *rec = (struct tbl_record *) record;
    struct table tbl = { .name = strdup(rec->name), .length = rec->length,
		         .clustered = rec->clustered, .col_count = 0 };
    tbl.col = malloc(sizeof(struct column) * rec->col_count);
    *col_count = rec->col_count;
    return tbl;
}

static
struct db *restore_db(char *record) {
    struct db_record *rec = (struct db_record *) record;
    struct db *db = malloc(sizeof *db);
    assert (db);
    db->table_count = 0;
    db->capacity = rec->tbl_count;
    db->tables = malloc(sizeof(struct table) * db->capacity);
    assert(db->tables);
    db->name = strdup(rec->name);
    return db;
}

static
struct db *restore(char *meta, size_t sz) {
    enum meta_state state = DB_RECORD;
    struct db *db;
    struct table *tbl;
    struct column *col;
    char *rec;
    size_t col_count = 0, prev = 0;
    char varbuf[VARNAME_SIZE];
    for (size_t j = 0; j < sz; j++) {
	if (meta[j] == '|') {
	    meta[j] = '\0';
	    rec = &meta[prev];
	    prev = j + 1;

	    switch(state) {
		case DB_RECORD:
		    db = restore_db(rec);
		    //map_insert(db->name, db, ENTITY);
		    state = TBL_RECORD;
		    break;
		case TBL_RECORD:
		    db->tables[db->table_count] = restore_tbl(rec, &col_count);
		    tbl = &db->tables[db->table_count++];
		    snprintf(varbuf, sizeof varbuf, "%s.%s", db->name, tbl->name);
		    map_insert(strdup(varbuf), tbl, ENTITY);
		    state = COL_RECORD;
		    break;
		case COL_RECORD:
		    tbl->col[tbl->col_count] = restore_col(rec, tbl);
		    col = &tbl->col[tbl->col_count++];
		    snprintf(varbuf, sizeof varbuf, "%s.%s.%s", db->name, tbl->name, col->name);
		    map_insert(strdup(varbuf), col, ENTITY);
		    if (col_count == tbl->col_count) state = TBL_RECORD;
		    break;
	    }
	}
    }

    assert(state == TBL_RECORD);
    return db;
}

static
struct db *restore_meta(void) {
    char buf[PATHLEN];
    char *path = vbsnprintf(buf, sizeof buf, "%s/%s.bin", DBPATH, METAFILE);

    char *meta;
    off_t sz = filemap(path, &meta, PROT_READ | PROT_WRITE);
    struct db *db = restore(meta, sz);
    fileunmap(meta, sz);
    return db;
}

//////////////////////////////////////////////////////////////////////////////

static
void restore_col_data(struct column *col) {
    char *tname = col->table->name;
    char buf[PATHLEN];
    char *path = vbsnprintf(buf, sizeof buf, "%s/%s.%s.bin", DBPATH, tname, col->name);

    char *data;
    off_t sz = filemap(path, &data, PROT_READ);
    memcpy(col->data.vals, data, sz);
    col->data.sz = sz / sizeof (int);
    col->data.capacity = col->data.sz;
    cs165_log(stdout, "col capacity %d\n", col->data.capacity);
    fileunmap(data, sz);
}

static
void restore_data(struct db *db) {
    for (size_t j = 0; j < db->table_count; j++) {
	struct table *tbl = &db->tables[j];
	for (size_t i = 0; i < tbl->col_count; i++)
	    restore_col_data(&tbl->col[i]);
    }
    return;
}

void restore_database(void) {
    struct stat st;
    if (stat(DBPATH, &st) == 0) {
	struct db *db = restore_meta();
	db_set_current(db);
	restore_data(db);
	struct table *tbl = &db->tables[0];
	for (size_t j = 0; j < tbl->col_count; j++)
	    fprintf(stderr, "%d ", tbl->col[j].data.vals[tbl->length-1]);
	fprintf(stderr, "\n");
    }
    return;
}