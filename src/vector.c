#include <stdbool.h>
#include <stdlib.h>
#include <stdio.h>
#include "vector.h"

#define DEFAULT_CAPACITY 8

bool vector_init(struct vec *v, size_t capacity) {
    v->sz = 0;
    v->capacity = (capacity < DEFAULT_CAPACITY) ? DEFAULT_CAPACITY : capacity;
    v->pos = malloc(sizeof(int) * v->capacity);
    if (v->pos == NULL) return true;
    return false;
}

bool vector_resize(struct vec *v) {
    int capacity = v->capacity * 2;
    int *pos = realloc(v->pos, sizeof(int) * capacity);
    if (pos == NULL) return false;
    v->capacity = capacity;
    v->pos = pos;
    return true;
}

unsigned int vector_append(struct vec *v, int item) {
    if (v->sz == v->capacity)
	if (!vector_resize(v)) return -1;

    v->pos[v->sz++] = item;
    return (v->sz - 1);
}

struct vec *vector_create(size_t capacity) {
    struct vec *v = malloc(sizeof *v);
    vector_init(v, capacity);
    // TODO think about the return status
    return v;
}
