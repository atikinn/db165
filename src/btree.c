#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <math.h>
#include "vector.h"
#include "bset.h"
#include "ccsort.h"
#include "btree.h"

/*
#define DEGREE 2
#define FANOUT (DEGREE * 2)
#define MAXKEYS (FANOUT - 1)
#define MINKEYS (DEGREE - 1)
*/

#define ORDER 16
#define MAXKEYS (ORDER * 2)
#define FANOUT (MAXKEYS + 1)
#define FANOUTSET (((FANOUT + 63) & ~(63))/64)

enum btnode_type { INTERNAL = 1, LEAF };

struct btnode {
    enum btnode_type ntype;
    short ksz;
    int keys[MAXKEYS];
    bset idref[FANOUTSET];  // only used in leafs to distinguish between id and ids
    union value {
	struct btnode *child;
	size_t id;
	struct vec *ids;
    } values[FANOUT];
};

struct split_pair {
    struct btnode *sibling;
    int pivot;
};

//////////////////////////////////////////////////////////////////////////////

static inline
struct btnode *allocate_btnode(enum btnode_type type) {
    struct btnode *node = malloc(sizeof *node);
    node->ntype = type;
    node->ksz = 0;
    if (type == LEAF) {
	memset(node->idref, 0, sizeof node->idref);
	node->values[FANOUT-1].child = NULL;
    }
    return node;
}

bool btree_init(struct btree *bt, bool leading) {
    bt->root = allocate_btnode(LEAF);
    if (bt->root == NULL) return true;
    bt->node_count = 1;
    bt->size = 0;
    bt->leading = leading;
    bt->iter = bt->root;
    return false;
}

struct btree *btree_create(bool leading) {
    struct btree *bt = malloc(sizeof *bt);
    if (bt == NULL) goto malloc_failed;

    bool err = btree_init(bt, leading);
    if (err) goto init_err;

    return bt;

init_err:
    free(bt);

malloc_failed:
    return NULL;
}

void btree_load(struct btree *bt, struct vec *v) {
    if (bt->leading) {
	for (int j = 0; j < v->sz; j++)
	    btree_insert(bt, v->pos[j], j);
    } else {	// or do bulk
	struct sorted_entry *zipped = zipWithIdx(v);
	ccqsort(zipped, v->sz, sizeof *zipped);
	for (int j = 0; j < v->sz; j++)
	    btree_insert(bt, zipped[j].value, zipped[j].clustered_id);
	free(zipped);
    }
}

struct btnode *btree_next_leaf(struct btnode *node) {
    return node->values[FANOUT-1].child;
}

int *btree_keysref(struct btnode *node) {
    return node->keys;
}

int btree_ksize(struct btnode *node) {
    return node->ksz;
}

//////////////////////////////////////////////////////////////////////////////

void print_vec(int *a, int sz) {
    printf("[ ");
    for (int i = 0; i < sz; i++)
        printf("%d ", a[i]);
    printf("]\n");
}

static
void traverse_bt(struct btnode *x) {
    if (x->ntype == INTERNAL) {
	for (int j = 0; j < x->ksz; j++)
	    fprintf(stderr, "%d ", x->keys[j]);
	fprintf(stderr, "\n\n");
	for (int j = 0; j <= x->ksz; j++)
	    traverse_bt(x->values[j].child);
    } else {
	for (int j = 0; j < x->ksz; j++) {
	    fprintf(stderr, "%d (", x->keys[j]);
	    if (bset_isset(x->idref, j)) {
		for (int i = 0; i < x->values[j].ids->sz; i++)
		    fprintf(stderr, " %d ", x->values[j].ids->pos[i]);
	    } else {
		fprintf(stderr, " %zu ", x->values[j].id);
	    }
	    fprintf(stderr, ") ");
	}
	fprintf(stderr, "\n");
    }
}

void btree_traverse(struct btree *bt) {
    traverse_bt(bt->root);
}
//////////////////////////////////////////////////////////////////////////////

//////////////////////////////////////////////////////////////////////////////

static inline
struct vec *get_vector(struct btnode *node, int i) {
    if (bset_isset(node->idref, i)) {
	return node->values[i].ids;
    } else {
	struct vec *v = vector_create(1);
	vector_push(v, node->values[i].id);
	return v;
    }
}

static inline
int indexOf(int key, int *a, int sz) {
    for (int j = 0; j < sz; j++)
	if (a[j] == key) return j;
    return -1;
    /*
    //binary search, inefficient on small arrays
    int lo = 0;
    int hi = sz - 1;
    while (lo <= hi) {
	int mid = lo + (hi - lo) / 2;
	if (key < a[mid]) hi = mid - 1;
	else if (key > a[mid]) lo = mid + 1;
	else return mid;
    }
    return -1;
    */
}

static
struct btnode *btnode_search(struct btnode *node, int k) {
    if (node == NULL) return NULL;
    int i = 0;
    switch (node->ntype) {
        case LEAF: return node;
        case INTERNAL:
            while (i < node->ksz && node->keys[i] < k) i++;
            return btnode_search(node->values[i].child, k);
    }
}

struct vec *btree_search(struct btree *bt, int k) {
    struct vec *result = NULL;
    if (bt == NULL) return result;

    struct btnode *leaf = btnode_search(bt->root, k);
    int i = indexOf(k, leaf->keys, leaf->ksz);
    if (i != -1)
	result = get_vector(leaf, i);
    return result;
}

static inline
void collect_ids(struct btnode *leaf, int lo, int hi, struct vec *v) {
    int i = 0;
    while (i < leaf->ksz && leaf->keys[i] < lo) i++;
    if (i == leaf->ksz) return;

    for (; i < leaf->ksz && leaf->keys[i] < hi; i++) {
	if (bset_isset(leaf->idref, i))
	    vector_append(v, leaf->values[i].ids);
	else
	    vector_push(v, leaf->values[i].id);
    }
}

struct vec *btree_rsearch(struct btree *bt, int lo, int hi) {
    assert(lo < hi);

    struct btnode *leaf = btnode_search(bt->root, lo);
    struct vec *v = vector_create(0);

    while (leaf != NULL && leaf->keys[0] < hi) {
	collect_ids(leaf, lo, hi, v);
	leaf = leaf->values[MAXKEYS].child;
    }

    return v;
}

//////////////////////////////////////////////////////////////////////////////

static inline
int key_insert(int *a, int lo, int hi, int k) {
    a[hi] = k;
    int j = hi;
    for (; j > lo && a[j] < a[j-1]; j--) {
        int swap = a[j];
        a[j] = a[j-1];
        a[j-1] = swap;
    }
    return j;
}

/* leaf nodes when value is not present */
static inline
int insert_id_atpos(union value *a, int pos, int sz, int id) {
    union value i = { .id = id };
    a[sz] = i;
    int j = sz;
    for (; j > pos; j--) {
        union value swap = a[j];
        a[j] = a[j-1];
        a[j-1] = swap;
    }
    return j;
}

/* leaf nodes when the values is already present */
static inline
void insert_id(union value *x, int id, bool idref) {
    if (idref) {
	vector_insert(x->ids, id);
	return;
    }

    struct vec *v = vector_create(2);
    if (x->id < id) {
	vector_push(v, x->id);
	vector_push(v, id);
    } else {
	vector_push(v, id);
	vector_push(v, x->id);
    }
    x->ids = v;
}

static inline
void increment_helper(struct btnode *leaf, int idx) {
    for (int j = idx; j < leaf->ksz; j++) {
	if (bset_isset(leaf->idref, j)) {
	    struct vec *v = leaf->values[j].ids;
	    for (int i = 0; i < v->sz; i++)
		v->pos[i]++;
	} else {
	    leaf->values[j].id++;
	}
    }
}

static inline
void increment_ids(struct btnode *leaf, int idx) {
    while (leaf != NULL) {
	increment_helper(leaf, idx);
	leaf = leaf->values[FANOUT-1].child;
	idx = 0;    // all leaves after the input will be incremented fully
    }
}

static inline
void leaf_insert(struct btnode *x, int k, int id, bool leading) {
    int kidx = indexOf(k, x->keys, x->ksz);
    if (kidx == -1) {
	kidx = key_insert(x->keys, 0, x->ksz, k);
	insert_id_atpos(x->values, kidx, x->ksz, id);
	x->ksz++;
	bset_insert0(x->idref, kidx, FANOUTSET);
	//print_vec(x->keys, x->ksz);
    } else {	// key exists, only add value
	//fprintf(stderr, "key %d exists at %d = %d\n", k, kidx, x->keys[kidx]);
	bool idref = bset_isset(x->idref, kidx);
	insert_id(&x->values[kidx], id, idref);
	if (!idref) bset_set(x->idref, kidx);
    }
    if (leading) increment_ids(x, kidx + 1);
}

static inline
struct split_pair leaf_split(struct btnode *full) {
    struct btnode *sibling = allocate_btnode(full->ntype);

    double fullksz = full->ksz;
    full->ksz = (int) floor(fullksz / 2);
    sibling->ksz = (int) ceil(fullksz / 2);

    for (int j = 0; j < sibling->ksz; j++)
	sibling->keys[j] = full->keys[j + full->ksz];
    for (int j = 0; j < sibling->ksz; j++)
	sibling->values[j] = full->values[j + full->ksz];

    bset rset[FANOUTSET] = {0};
    for (int j = 0; j < sibling->ksz; j++)
	if (bset_isset(full->idref, j + full->ksz))
	    bset_set(sibling->idref, j);

    full->values[MAXKEYS].child = sibling;
    struct split_pair ret = { sibling, sibling->keys[0] };
    return ret;
}

static inline
struct split_pair insert_leaf_helper(struct btnode *node, int k, int id, bool leading) {
    struct split_pair null = {};
    if (node->ksz == MAXKEYS) {
	struct split_pair split_result = leaf_split(node);
	//fprintf(stderr, "insert_leaf_helper: %d\n", split_result.sibling->ksz);
	if (k < split_result.pivot)
	    leaf_insert(node, k, id, leading);
	else
	    leaf_insert(split_result.sibling, k, id, leading);
	return split_result;
    } else {
	leaf_insert(node, k, id, leading);
	return null;
    }
}

/* for internal node ptr inserts */
static inline
int insert_ptr_atpos(union value *a, int pos, int sz, struct btnode *p) {
    union value v = { .child = p };
    a[sz] = v;
    int j = sz;
    for (; j > pos; j--) {
        union value swap = a[j];
        a[j] = a[j-1];
        a[j-1] = swap;
    }
    return j;
}

static inline
void node_insert(struct btnode *x, int pivot, struct btnode *p) {
    int pidx = key_insert(x->keys, 0, x->ksz, pivot);
    x->ksz++; // NB: important to increment before calling insert_ptr_atpos
    insert_ptr_atpos(x->values, pidx+1, x->ksz, p);
}

static inline
struct split_pair node_split(struct btnode *full) {
    struct btnode *sibling = allocate_btnode(full->ntype);

    double fullksz = full->ksz;
    full->ksz = (int) floor(fullksz / 2);
    sibling->ksz = (int) ceil(fullksz / 2) - 1;

    for (int j = 0; j < sibling->ksz; j++)
	sibling->keys[j] = full->keys[j + full->ksz+1];
    for (int j = 0; j < sibling->ksz+1; j++)
	sibling->values[j] = full->values[j + full->ksz+1];

    struct split_pair ret = { sibling, full->keys[full->ksz] };
    return ret;
}

static inline
int redistribute(struct btnode *left, struct btnode *right, int pivot) {
    assert(left->ksz > right->ksz);
    struct btnode *ptr = left->values[left->ksz].child;
    left->ksz--;
    int new_pivot = left->keys[left->ksz];
    node_insert(right, pivot, ptr);
    return new_pivot;
}

static inline
struct split_pair insert_node_helper(struct btnode *node, struct btnode *sibling, int pivot) {
    struct split_pair null = {};
    if (node->ksz == MAXKEYS) {
	struct split_pair split_result = node_split(node);
	if (pivot < split_result.pivot)
	    node_insert(node, pivot, sibling);
	else
	    node_insert(split_result.sibling, pivot, sibling);

	if (node->ksz != split_result.sibling->ksz)
	    split_result.pivot = redistribute(node, split_result.sibling, split_result.pivot);
	assert(node->ksz == split_result.sibling->ksz);
	return split_result;
    } else {
	node_insert(node, pivot, sibling);
	return null;
    }
}

static
struct split_pair insert(struct btnode *node, int k, int id, bool leading) {
    struct split_pair null = { 0 };
    if (node->ntype == INTERNAL) {
	int i = 0;
	while (i < node->ksz && k >= node->keys[i]) i++;
	struct split_pair result = insert(node->values[i].child, k, id, leading);
	if (result.sibling == NULL) return null; // no split
	return insert_node_helper(node, result.sibling, result.pivot);
    } else {
	return insert_leaf_helper(node, k, id, leading);
    }
}

void btree_insert(struct btree *bt, int k, int id) {
    assert(bt->root != NULL);
    struct split_pair p = insert(bt->root, k, id, bt->leading);
    //fprintf(stderr, "insert: %p, %d; keys = ", p.sibling, p.pivot);
    //print_vec(bt->root->keys, bt->root->ksz);
    if (p.sibling != NULL) {
	struct btnode *root = allocate_btnode(INTERNAL);
	root->keys[root->ksz] = p.pivot;
	root->values[root->ksz].child = bt->root;
	root->ksz++;
	root->values[root->ksz].child = p.sibling;
	bt->root = root;
	bt->node_count++;
	//print_vec(p.sibling->keys, p.sibling->ksz);
	//fprintf(stderr, "new root: ");
	//print_vec(root->keys, root->ksz);
    }
    bt->size++;
}
