#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <assert.h>
#include <math.h>
#include "vector.h"
#include "btree.h"

/*
#define DEGREE 2
#define FANOUT (DEGREE * 2)
#define MAXKEYS (FANOUT - 1)
#define MINKEYS (DEGREE - 1)
*/

#define ORDER 2
#define MAXKEYS (ORDER * 2)
#define FANOUT (MAXKEYS + 1)

enum btnode_type { INTERNAL = 1, LEAF };
enum values_type { ID = 1, IDREF };

// sizeof = 4 * (MAXKEYS + 1) + 16 * FANOUT
// sizeof = 4 * (MAXKEYS + 1) + 16 * (MAXKEYS + 1)
// sizeof = 20 * (MAXKEYS + 1)
// sizeof = 20 * (2 * ORDER + 1)
// sizeof = 20 + 40 ORDER
// ORDER  2 => 100 => 104 => 2 cache lines
// ORDER  3 => 160  => 2.5 cache lines => 3 cache lines
// ORDER 10 => 420 => 424 => 7 cache lines
// ORDER 100 => 4024 => 63 cache lines
// ORDER 101 => 4060 => 64 cache lines

// can do
// sizeof 32 + 24 ORDER
// ORDER 169 => 64 cache lines

struct btnode {
    enum btnode_type ntype;
    short ksz;
    int keys[MAXKEYS];
    struct value {
        enum values_type vtype;
        union {
            struct btnode *child;
            unsigned int id;
            struct vec *ids;
        };
    } values[FANOUT];
};

struct insert_pair {
    struct btnode *sibling;
    int pivot;
};

//////////////////////////////////////////////////////////////////////////////

static inline
struct btnode *allocate_btnode(enum btnode_type type) {
    struct btnode *node = malloc(sizeof *node);
    node->ntype = type;
    node->ksz = 0;
    return node;
}

bool btree_init(struct btree *bt) {
    bt->root = allocate_btnode(LEAF);
    if (bt->root == NULL) return true;
    return false;
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
	    switch (x->values[j].vtype) {
		case ID: fprintf(stderr, " %d ", x->values[j].id); break;
		case IDREF:
		    for (int i = 0; i < x->values[j].ids->sz; i++)
			fprintf(stderr, " %d ", x->values[j].ids->pos[i]);
		    break;
	    }
	    fprintf(stderr, ")");
	}
	fprintf(stderr, "\n");
    }
}

void btree_traverse(struct btree *bt) {
    traverse_bt(bt->root);
}

//////////////////////////////////////////////////////////////////////////////

static inline
struct vec *get_vector(struct btnode *node, int i) {
    if (node->values[i].vtype == IDREF) {
	return node->values[i].ids;
    } else {
	struct vec *v = vector_create(1);
	vector_append(v, node->values[i].id);
	return v;
    }
}

static inline
int indexOf(int key, int *a, int sz) {
    int lo = 0;
    int hi = sz - 1;
    while (lo <= hi) {
	int mid = lo + (hi - lo) / 2;
	if (key < a[mid]) hi = mid - 1;
	else if (key > a[mid]) lo = mid + 1;
	else return mid;
    }
    return -1;
}

static
struct btnode *btnode_search(struct btnode *node, int k) {
    if (node == NULL) return NULL;
    int i = 0;
    switch (node->ntype) {
        case INTERNAL:
            while (i < node->ksz && k >= node->keys[i]) i++;
            return btnode_search(node->values[i].child, k);
        case LEAF: return node;
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

//////////////////////////////////////////////////////////////////////////////

static inline
int array_insert(int *a, int lo, int hi, int k) {
    a[hi] = k;
    int j = hi;
    for (; j > lo && a[j] < a[j-1]; j--) {
        int swap = a[j];
        a[j] = a[j-1];
        a[j-1] = swap;
    }
    return j;
}

static inline
int insert_id_atpos(struct value *a, int lo, int hi, int v) {
    struct value i = { ID, .id = v };
    a[hi] = i;
    int j = hi;
    for (; j > lo; j--) {
        struct value swap = a[j];
        a[j] = a[j-1];
        a[j-1] = swap;
    }
    return j;
}

static inline
int insert_ptr_atpos(struct value *a, int lo, int hi, struct btnode *p) {
    struct value v = { ID, .child = p };
    a[hi] = v;
    int j = hi;
    for (; j > lo; j--) {
        struct value swap = a[j];
        a[j] = a[j-1];
        a[j-1] = swap;
    }
    return j;
}

static inline
void insert_id(struct value *x, int id) {
    if (x->vtype == IDREF) {
	vector_append(x->ids, id);
    } else {
	struct vec *v = vector_create(2);
	vector_append(v, x->id);
	vector_append(v, id);
	x->ids = v;
	x->vtype = IDREF;
    }
}

static inline
void leaf_insert(struct btnode *x, int k, int id) {
    int kidx = indexOf(k, x->keys, x->ksz);
    //fprintf(stderr, "leaf_insert: %d = %d, %d, %d\n", k, id, kidx, x->ksz);
    if (kidx == -1) {
	kidx = array_insert(x->keys, 0, x->ksz, k);
	insert_id_atpos(x->values, kidx, x->ksz, id);
	x->ksz++;
	//print_vec(x->keys, x->ksz);
    } else {	// key exists, only add value
	//fprintf(stderr, "key %d exists at %d = %d\n", k, kidx, x->keys[kidx]);
	insert_id(&x->values[kidx], id);
    }
}

static inline
void node_insert(struct btnode *x, int pivot, struct btnode *p) {
    int pidx = array_insert(x->keys, 0, x->ksz, pivot);
    x->ksz++; // NB: important to increment before calling insert_ptr_atpos
    insert_ptr_atpos(x->values, pidx+1, x->ksz, p);
}

static inline
struct insert_pair node_split(struct btnode *full) {
    struct btnode *sibling = allocate_btnode(full->ntype);

    double fullksz = full->ksz;
    if (full->ntype == LEAF) {
	full->ksz = (int) floor(fullksz / 2);
	sibling->ksz = (int) ceil(fullksz / 2);

	for (int j = 0; j < sibling->ksz; j++)
	    sibling->keys[j] = full->keys[j + full->ksz];
	for (int j = 0; j < sibling->ksz; j++)
	    sibling->values[j] = full->values[j + full->ksz];

	full->values[FANOUT-1].child = sibling;
	struct insert_pair ret = { sibling, sibling->keys[0] };
	return ret;
    } else {
	full->ksz = (int) floor(fullksz / 2);
	sibling->ksz = (int) ceil(fullksz / 2) - 1;

	for (int j = 0; j < sibling->ksz; j++)
	    sibling->keys[j] = full->keys[j + full->ksz+1];
	for (int j = 0; j < sibling->ksz+1; j++)
	    sibling->values[j] = full->values[j + full->ksz+1];
	struct insert_pair ret = { sibling, full->keys[full->ksz] };
	return ret;
    }

    //fprintf(stderr, "node_split: %d\n", sibling->keys[0]);
}

static inline
struct insert_pair insert_leaf_helper(struct btnode *node, int k, int id) {
    struct insert_pair null = {};
    if (node->ksz == MAXKEYS) {
	struct insert_pair split_result = node_split(node);
	//fprintf(stderr, "insert_leaf_helper: %d\n", split_result.sibling->ksz);
	if (k < split_result.pivot)
	    leaf_insert(node, k, id);
	else
	    leaf_insert(split_result.sibling, k, id);
	return split_result;
    } else {
	leaf_insert(node, k, id);
	return null;
    }
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
struct insert_pair insert_node_helper(struct btnode *node, struct btnode *sibling, int pivot) {
    struct insert_pair null = {};
    if (node->ksz == MAXKEYS) {
	struct insert_pair split_result = node_split(node);
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
struct insert_pair insert(struct btnode *node, int k, int id) {
    struct insert_pair null = {};
    if (node->ntype == INTERNAL) {
	int i = 0;
	while (i < node->ksz && k >= node->keys[i]) i++;
	struct insert_pair result = insert(node->values[i].child, k, id);
	if (result.sibling == NULL) return null; // no split
	return insert_node_helper(node, result.sibling, result.pivot);
    } else {
	return insert_leaf_helper(node, k, id);
    }
}

void btree_insert(struct btree *bt, int k, int id) {
    assert(bt->root != NULL);
    struct insert_pair p = insert(bt->root, k, id);
    //fprintf(stderr, "insert: %p, %d; keys = ", p.sibling, p.pivot);
    //print_vec(bt->root->keys, bt->root->ksz);
    if (p.sibling != NULL) {
	struct btnode *root = allocate_btnode(INTERNAL);
	root->keys[root->ksz] = p.pivot;
	root->values[root->ksz].child = bt->root;
	root->ksz++;
	root->values[root->ksz].child = p.sibling;
	bt->root = root;
	//print_vec(p.sibling->keys, p.sibling->ksz);
	fprintf(stderr, "new root: ");
	print_vec(root->keys, root->ksz);
    }
    bt->size++;
}
