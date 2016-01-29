#include <stdlib.h>
#include <stdbool.h>
#include <assert.h>

#include "sindex.h"
#include "utils.h"

/*
enum sindex_idtype { ID, IDS };

struct element {
    enum sindex_idtype;
    int val;
    union {
        unsigned rid;
        struct vec *rids;
    }
}
*/
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

void sindex_free(struct sindex *idx){
    //TODO free *rids
    //free(idx->vals);
    free(idx);
}

struct sindex *sindex_alloc(size_t data_sz) {
    size_t sz = next_pow2(data_sz);
    struct sindex *zip = malloc(sz * sizeof *zip);
    assert(zip);
    return zip;
}

int sindex_val_cmp(const void *a, const void *b) {
    const struct sindex *p1 = a;
    const struct sindex *p2 = b;
    return (p1->val < p2->val) ? -1 : (p1->val > p2->val) ? 1 : 0;
}

bool is_sorted(struct sindex *idx, size_t sz) {
    for (size_t i = 1; i < sz; i++)
        if (idx[i].val < idx[i-1].val) return false;
    return true;
}

struct sindex *sindex_create(struct vec *data) {
    size_t sz = next_pow2(data->sz);
    struct sindex *zip = malloc(sz * sizeof *zip);
    assert(zip);
    for (size_t j = 0; j < data->sz; j++) {
        zip[j].val = data->vals[j];
        zip[j].pos = j;
    }

    qsort(zip, data->sz, sizeof *zip, sindex_val_cmp);
    //assert(is_sorted(zip, data->sz));

    return zip;
}

struct sindex *sindex_insert(struct sindex *idx, unsigned pos, int val, size_t sz) {
    struct sindex *index = idx;
    if (sz >= next_pow2(sz))
	index = realloc(idx, sizeof(struct sindex) * next_pow2(sz));

    index[sz].val = val;
    index[sz].pos = pos;

    size_t j = sz;
    for (; j > 0 && index[j].val < index[j-1].val; j--) {
        struct sindex swap = index[j];
        index[j] = index[j-1];
        index[j-1] = swap;
    }

    for (size_t i = 0; i <= sz; i++)
        if (index[i].pos >= j && index[i].val != val) index[i].pos++;

    return index;
}

static
int sindex_of_left(struct sindex *a, int left_range, int length) {
    if (a[length-1].val < left_range) return -1;

    int low = 0;
    int high = length-1;

    while (low <= high) {
        int mid = low + ((high - low) / 2);

        if (a[mid].val >= left_range)
            high = mid - 1;
        else //if(a[mid]<i)
            low = mid + 1;
    }

    return high + 1;
}

static
int sindex_of_right(struct sindex *a, int right_range, int length) {
    if (a[0].val > right_range) return -1;

    int low = 0;
    int high = length - 1;

    while (low <= high) {
        int mid = low + ((high - low) / 2);

        if (a[mid].val > right_range)
            high = mid - 1;
        else //if(a[mid]<i)
            low = mid + 1;
    }

    return low - 1;
}

size_t sindex_scan(int **v, int low, int high, size_t sz, struct sindex *idx) {
    int low_idx = sindex_of_left(idx, low, sz);
    int high_idx = sindex_of_right(idx, high, sz);
    //bool flag = low_idx == -1 || high_idx == -1 || low_idx > high_idx;

    //size_t num_tuples = flag ? 0 : high_idx - low_idx + 1;
    size_t num_tuples = low_idx > high_idx ? 0 : high_idx - low_idx + 1;
    int *vec = malloc(num_tuples * sizeof *vec);

    for (size_t j = 0, k = low_idx; j < num_tuples; j++)
        vec[j] = idx[k+j].pos;
    /*
    for (size_t i = 0; i < num_tuples; i++) {
        cs165_log(stderr, "sindex_scan: vec[%d] = %d\n", i, vec[i]);
    }
    */
    *v = vec;
    return num_tuples;
}

static
int sindex_index_of(struct sindex *a, int key, int sz) {
    int lo = 0;
    int hi = sz;
    while (lo <= hi) {
        size_t mid = lo + (hi - lo) / 2;
        if      (key < a[mid].val) hi = mid - 1;
        else if (key > a[mid].val) lo = mid + 1;
        else return mid;
    }
    return -1;
}

static inline
int sindex_search_until(struct sindex *vals, size_t sz, int start_idx, int value, bool up) {
    size_t j;
    if (up == true) {
        for (j = start_idx; j < sz; j++)
            if (vals[j].val != value) break;
        return j - 1;
    } else {
        for (j = start_idx; j > 0; j--)
            if (vals[j].val != value) break;
        return (vals[0].val == value) ? 0 : j + 1;
    }
}

size_t sindex_find(int **v, int value, size_t sz, struct sindex *idx) {
    int ret = sindex_index_of(idx, value, sz);

    if (ret == -1) {
        *v = NULL;
        return 0;
    }

    int low_idx = sindex_search_until(idx, sz, ret, value, false);
    int high_idx = sindex_search_until(idx, sz, ret, value, true);
    size_t num_tuples = high_idx - low_idx + 1;

    cs165_log(stderr, "select_clustered: %zu %zu %zu\n", low_idx, high_idx, num_tuples);
    int *vec = malloc(num_tuples * sizeof *vec);
    for (size_t j = 0, k = low_idx; j < num_tuples; j++)
        vec[j] = idx[k+j].pos;
    *v = vec;
    return num_tuples;
}
