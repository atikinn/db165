#ifndef VEC_H
#define VEC_H

struct vec {
    size_t sz;
    size_t capacity;
    int *pos;
};

extern bool vector_init(struct vec *v, size_t capacity);
extern bool vector_resize(struct vec *v);
extern unsigned int vector_append(struct vec *v, int item);
extern struct vec *vector_create(size_t capacity);
#endif
