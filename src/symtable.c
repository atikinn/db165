#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

#include "symtable.h"
#include "utils.h"
#include "sfhash.h"

/** Symbol hash table for variables **/
//const int VAR_MAP_SIZE = 1024;
#define VAR_MAP_SIZE 1024
static struct {
    size_t num_elems;
    size_t capacity;
    struct element {
        uint32_t keyhash;
        const char *key;
        void *value;
    } *data;
} var_map[VAR_MAP_SIZE];

void *map_get(const char *var) {
    size_t idx = super_fast_hash(var, strlen(var));
    size_t num_elements = var_map[idx].num_elems;
    uint32_t var_hash = 0;
    strncpy((char *) &var_hash, var, sizeof var_hash);
    for (size_t i = 0; i < num_elements; i++) {
        struct element *e = &var_map[idx].data[i];
        if (e->keyhash == var_hash && !strcmp(e->key, var)) return e->value;
    }
    return NULL;
}

/*
 * |var| should be malloced in advance (e.x. come from dbo) and not freed
 */
bool map_insert(const char *var, void *value) {
    if (map_get(var) != NULL) return false;
    size_t idx = super_fast_hash(var, strlen(var));
    size_t num_elems = var_map[idx].num_elems;
    size_t capacity = var_map[idx].capacity;
    if (num_elems >= capacity) {
        size_t new_capacity = capacity ? capacity * 2 : 8;
        var_map[idx].data = realloc(var_map[idx].data, sizeof(struct element) * new_capacity);
        var_map[idx].capacity = new_capacity;
    }
    uint32_t keyhash = 0;
    strncpy((char *) &keyhash, var, sizeof keyhash);
    var_map[idx].data[num_elems].keyhash = keyhash;
    var_map[idx].data[num_elems].value = value;
    var_map[idx].data[num_elems].key = var;
    var_map[idx].num_elems++;
    return true;
}

