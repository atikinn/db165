#ifndef CCSORT_H
#define CCSORT_H

struct sorted_entry {
    int value;
    int clustered_id;
};

extern void ccqsort(struct sorted_entry *x, int sz, int elem_size);
extern void ccsort(struct sorted_entry **zipped, int sz, int elem_size);
extern struct sorted_entry *zipWithIdx(struct vec *v);

#endif
