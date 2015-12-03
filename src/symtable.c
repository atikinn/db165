#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

#include "symtable.h"
#include "utils.h"
#include "sfhash.h"

/** Symbol hash table for variables **/
#define VAR_MAP_SIZE 1024

static struct {
    size_t num_elems;
    size_t capacity;
    struct element {
        uint64_t keyhash;
        char *key;
        enum vartype keytype;
        void *value;
    } *data;
} var_map[VAR_MAP_SIZE];

void *map_get(const char *var) {
    size_t idx = super_fast_hash(var, strlen(var)) % VAR_MAP_SIZE;
    size_t num_elements = var_map[idx].num_elems;
    uint64_t var_hash = 0;
    strncpy((char *) &var_hash, var, sizeof var_hash);
    cs165_log(stderr, "search for %s -> %lu in %d\n", var, var_hash, idx);
    for (size_t i = 0; i < num_elements; i++) {
        struct element e = var_map[idx].data[i];
        cs165_log(stderr, "check %s -> %lu with value %p\n", e.key, e.keyhash, e.value);
        if (e.keyhash == var_hash && !strcmp(e.key, var))
            return e.value;
    }
    cs165_log(stderr, "search: not found\n");
    return NULL;
}

/*
 * |key| should be malloced in advance (e.x. come from dbo) and not freed
 */
bool map_insert(char *key, void *value, enum vartype type) {
    //if (map_get(var) != NULL) return false;
    size_t idx = super_fast_hash(key, strlen(key)) % VAR_MAP_SIZE;
    size_t num_elems = var_map[idx].num_elems;
    size_t capacity = var_map[idx].capacity;
    if (num_elems >= capacity) {
        size_t new_capacity = capacity ? capacity * 2 : 8;
        struct element *data =
            realloc(var_map[idx].data, sizeof(struct element) * new_capacity);
        var_map[idx].data = data;
        var_map[idx].capacity = new_capacity;
    }
    uint64_t keyhash = 0;
    strncpy((char *) &keyhash, key, sizeof keyhash);
    var_map[idx].data[num_elems].keyhash = keyhash;
    var_map[idx].data[num_elems].value = value;
    var_map[idx].data[num_elems].key = key;
    var_map[idx].data[num_elems].keytype = type;
    var_map[idx].num_elems++;
    cs165_log(stderr, "inserted %s -> %p in %d with key %lu\n", key, value, idx, keyhash);
    return true;
}

void clean_symtbl(void) {
    for (size_t i = 0; i < VAR_MAP_SIZE; i++) {
        for (size_t j = 0; j < var_map[i].num_elems; j++) {
            struct element *e = &var_map[i].data[j];
            free(e->key);
            if (e->keytype == RESULT) free(e->value);
        }
        if (var_map[i].num_elems != 0) free(var_map[i].data);
    }
    return;
}
