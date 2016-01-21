#ifndef SINDEX_H
#define SINDEX_H

#include "vector.h"

struct sindex {
    int val;
    unsigned pos;
};

extern int sindex_val_cmp(const void *a, const void *b);
extern struct sindex *sindex_create(struct vec *data);
extern struct sindex *sindex_insert(struct sindex *idx, unsigned pos, int val, size_t sz);
extern size_t sindex_scan(int **v, int low, int high, size_t sz, struct sindex *idx);
extern struct sindex *sindex_alloc(size_t data_sz);

#endif

