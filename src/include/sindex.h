#ifndef SINDEX_H
#define SINDEX_H

#include "vector.h"

/*
struct sidx {
    size_t sz;
    size_t capacity;
    struct ssindex *vals;
}

struct ssindex {
    int val;
    union {
        unsigned rid;
        struct vec *rids;
    }
}
*/
struct sindex {
    int val;
    unsigned pos;
};

extern void sindex_free(struct sindex *idx);
extern int sindex_val_cmp(const void *a, const void *b);
extern struct sindex *sindex_create(struct vec *data);
extern struct sindex *sindex_insert(struct sindex *idx, unsigned pos, int val, size_t sz);
extern size_t sindex_scan(int **v, int low, int high, size_t sz, struct sindex *idx);
extern struct sindex *sindex_alloc(size_t data_sz);
extern bool is_sorted(struct sindex *idx, size_t sz);

#endif

