#ifndef BTREE_H
#define BTREE_H

struct btree {
    struct btnode *root;
    size_t size;
    size_t node_count;
    struct btnode *iter;
    bool leading;
};

extern struct btree *btree_create(bool clustered);
extern bool btree_init(struct btree *bt, bool clustered);
extern void btree_load(struct btree *bt, struct vec *v);
extern void btree_insert(struct btree *bt, int k, int id);
extern struct vec *btree_search(struct btree *bt, int k);
extern struct vec *btree_rsearch(struct btree *bt, int lo, int hi);

extern void btree_traverse(struct btree *bt);
extern struct btnode *btree_next_leaf(struct btnode *iter);

extern void btree_serial(struct btree *bt, void *file);
extern void btree_deserial(void *file, struct btree *bt);

extern int *btree_keysref(struct btnode *node);
extern int btree_ksize(struct btnode *node);

#endif
