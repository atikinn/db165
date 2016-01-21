#include <stdbool.h>
#include <stdlib.h>
#include <stdio.h>
#include "vector.h"
#include <assert.h>

//#include "cs165_api"

#define DEFAULT_CAPACITY 8

bool vector_init(struct vec *v, size_t capacity) {
    v->sz = 0;
    v->capacity = (capacity < DEFAULT_CAPACITY) ? DEFAULT_CAPACITY : capacity;
    v->vals = malloc(sizeof(int) * v->capacity);
    if (v->vals == NULL) return true;
    return false;
}

struct vec *vector_create(size_t capacity) {
    struct vec *v = malloc(sizeof *v);
    bool err = vector_init(v, capacity);
    if (err) {
	free(v);
	return NULL;
    }
    return v;
}

bool vector_resize(struct vec *v) {
    int capacity = v->capacity * 2;
    int *vals = realloc(v->vals, sizeof(int) * capacity);
    if (vals == NULL) return false;
    v->capacity = capacity;
    v->vals = vals;
    return true;
}

size_t vector_push(struct vec *v, int item) {
    if (v->sz == v->capacity)
	if (!vector_resize(v)) return -1;

    v->vals[v->sz++] = item;
    return (v->sz - 1);
}

size_t vector_insert(struct vec *v, int item, size_t pos) {
    size_t j = vector_push(v, item);
    for (; j > pos; j--) {
        int swap = v->vals[j];
        v->vals[j] = v->vals[j-1];
        v->vals[j-1] = swap;
    }
    assert(j == pos);
    return j;
}

size_t vector_insert_sorted(struct vec *v, int item) {
    size_t j = vector_push(v, item);
    for (; j > 0 && v->vals[j] < v->vals[j-1]; j--) {
        int swap = v->vals[j];
        v->vals[j] = v->vals[j-1];
        v->vals[j-1] = swap;
    }
    return j;
}

size_t vector_append(struct vec *v1, struct vec *v2) {
    for (size_t j = 0; j < v2->sz; j++)
	vector_push(v1, v2->vals[j]);
    return v1->sz;
}

