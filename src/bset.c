#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include "bset.h"

void bset_set(bset *set, int idx) {
    set[idx/32] |= (1 << (idx % 32));
}

void bset_unset(bset *set, int idx) {
    set[idx/32] &= ~(1 << (idx % 32));
}

bool bset_isset(bset *set, int idx) {
    return (set[idx/32] & (1 << (idx % 32)));
}

void bset_insert0(bset *set, int idx, size_t maxsz) {
    bset new[maxsz];

    memset(new, 0, sizeof new);

    for (int i = 0; i < idx; i++)
	if (bset_isset(set, i)) bset_set(new, i);

    for (unsigned long i = idx; i < (maxsz * 32) - 1; i++)
	if (bset_isset(set, i)) bset_set(new, i+1);

    memcpy(set, new, sizeof new);
}
