#ifndef VEC_H
#define VEC_H

struct vec {
    size_t sz;
    size_t capacity;
    int *vals;
};

extern bool vector_init(struct vec *v, size_t capacity);
extern bool vector_resize(struct vec *v);
extern size_t vector_push(struct vec *v, int item);
extern struct vec *vector_create(size_t capacity);
extern size_t vector_append(struct vec *v1, struct vec *v2);
extern size_t vector_insert(struct vec *v, int item);

#endif
