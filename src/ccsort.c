#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include "vector.h"
#include "assert.h"
#include "ccsort.h"

#define CACHELINE 64
#define CACHESIZE (500 * CACHELINE)

void print_arr(int *a, int sz) {
    printf("[ ");
    for (int i = 0; i < sz; i++)
        printf("%d ", a[i]);
    printf("]\n");
}

int value_compar(const void *a, const void *b) {
    return ((*(struct sorted_entry*)a).value - (*(struct sorted_entry*)b).value);
}

struct sorted_entry *zipWithIdx(struct vec *v) {
    struct sorted_entry *zipped = malloc(sizeof(struct sorted_entry) * v->sz);
    assert (zipped != NULL);
    for (int j = 0; j < v->sz; j++) {
	zipped[j].value = v->pos[j];
	zipped[j].clustered_id = j;
    }
    return zipped;
}

void print_buf(struct sorted_entry *e, int sz) {
    printf("[ ");
    for (int i = 0; i < sz; i++)
        printf("%d ", e[i].value);
    printf("]\n");
}

static
void merge(struct sorted_entry *src, int lo, int mid, int hi, struct sorted_entry *dst) {
    int i = lo, j = mid + 1;
    for (int k = lo; k <= hi; k++) {
	if      (i > mid)		      dst[k] = src[j++];
	else if (j > hi)		      dst[k] = src[i++];
	else if (src[j].value < src[i].value) dst[k] = src[j++];   // to ensure stability
	else				      dst[k] = src[i++];
    }
    return;
}

static
void two_way_merge(struct sorted_entry *src, int lo, int mid, int hi, struct sorted_entry *dst) {
    merge(src, lo, mid, hi, dst);
}

static
int qsort_in_cache(struct sorted_entry *arr, int elems_per_cache, int sz, int elem_size) {
    int runs = 0;
    for (int j = 0; j < sz; j += elems_per_cache, runs++) {
	int elem_to_move = (sz - j < elems_per_cache) ? (sz - j) : elems_per_cache;
	qsort(arr, elem_to_move, elem_size, value_compar);
	arr += elems_per_cache;
    }
    return runs;
}

void ccqsort(struct sorted_entry *x, int sz, int elem_size) {
    qsort(x, sz, elem_size, value_compar);
}

/* two-way cache-friendly merge sort */
void ccsort(struct sorted_entry **x, int sz, int elem_size) {
    struct sorted_entry *arr = *x;
    int elems_per_cache = CACHESIZE/elem_size;

    struct sorted_entry *buf = malloc(sz * sizeof *buf);
    int runs = qsort_in_cache(arr, elems_per_cache, sz, elem_size);
    int mult = 1;
    int rounds = 0;
    while (runs > 1) {
	for (int r = 0; r < runs; r += 2) {
	    int lo = r * elems_per_cache * mult;
	    int mid = (r + 1) * (elems_per_cache * mult) - 1;
	    int hi = (lo + 2 * mult * elems_per_cache) - 1;
	    if (hi > sz) hi = sz - 1;
	    if (runs - r > 1) {
		two_way_merge(arr, lo, mid, hi, buf);
	    } else {
		int remaining = sz - (r * elems_per_cache * mult);
		memcpy(buf + lo, arr + lo, remaining * elem_size);
	    }
	}

	runs = runs / 2 + runs % 2;
	mult *= 2;
	rounds++;

	struct sorted_entry *temp = arr;
	arr = buf;
	buf = temp;
    }

    free(buf);
    *x = arr;
}

