#ifndef BSET_H
#define BSET_H

typedef unsigned int bset;

extern void bset_set(bset *set, int idx);
extern void bset_unset(bset *set, int idx);
extern bool bset_isset(bset *set, int idx);
void bset_insert0(bset *set, int idx, size_t capacity);

#endif
