#ifndef SYMT_H
#define SYMT_H

#include <stdbool.h>

extern void *map_get(const char *var);
extern bool map_insert(const char *var, void *value);

#endif
