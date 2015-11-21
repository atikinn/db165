#ifndef QUEUE_H
#define QUEUE_H

struct queue {
    size_t sz;
    size_t capacity;
    size_t first;
    size_t last;
    struct btnode **nodes;
};

extern struct queue *queue_create(size_t capacity);
extern bool queue_init(struct queue *q, size_t capacity);
extern bool queue_is_empty(struct queue *q);

extern void queue_enque(struct queue *q, struct btnode *node);
extern struct btnode *queue_dequeue(struct queue *q);
extern struct btnode *queue_peek(struct queue *q);

#endif
