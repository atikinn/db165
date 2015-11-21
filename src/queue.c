#include <stdlib.h>
#include <stdbool.h>
#include "queue.h"

bool queue_init(struct queue *q, size_t capacity) {
    q->capacity = (capacity < 2) ? 2 : capacity;

    q->nodes = malloc(q->capacity * sizeof *q->nodes);
    if (q->nodes == NULL) return true;

    q->sz = 0;
    q->first = 0;
    q->last = 0;

    return false;
}

struct queue *queue_create(size_t capacity) {
    struct queue *q = malloc(sizeof *q);
    if (q == NULL) return NULL;

    bool err = queue_init(q, capacity);
    if (err) {
	free(q);
	return NULL;
    }

    return q;
}

bool queue_is_empty(struct queue *q) {
    return q->sz == 0;
}

static inline
void resize(struct queue *q, size_t new_capacity) {
    struct btnode **ns = realloc(q->nodes, new_capacity * sizeof *ns);

    for (int j = 0; j < q->sz; j++)
	ns[j] = ns[(q->first + j) % q->capacity];

    q->nodes = ns;
    q->capacity = new_capacity;
    q->first = 0;
    q->last = q->sz;
}

void queue_enque(struct queue *q, struct btnode *item) {
    if (q->sz == q->capacity) resize(q, q->capacity * 2);
    q->nodes[q->last++] = item;
    if (q->last == q->capacity) q->last = 0;
    q->sz++;
}

struct btnode *queue_dequeue(struct queue *q) {
    if (q->sz == 0) return NULL;

    struct btnode *node = q->nodes[q->first];
    q->nodes[q->first] = NULL;
    q->sz--;
    q->first++;
    if (q->first == q->capacity) q->first = 0;
    if (q->sz > 0 && q->sz == q->capacity / 4) resize(q, q->capacity / 2);
    return node;
}

struct btnode *queue_peek(struct queue *q) {
    return q->nodes[q->first];
}
