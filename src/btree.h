#ifndef BTREE_H
#define BTREE_H

struct btree {
    struct btnode *root;
	size_t size;
};

extern void btree_insert(struct btree *bt, int k, int id);
extern struct vec *btree_search(struct btree *bt, int k);
extern void btree_traverse(struct btree *bt);
extern bool btree_init(struct btree *bt);
extern struct btree *btree_create();
#endif
