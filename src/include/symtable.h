#ifndef SYMT_H
#define SYMT_H

#include <stdbool.h>

enum vartype { INVALID_VARTYPE, ENTITY, RESULT };

extern enum vartype map_gettype(const char *var);
extern void *map_get(const char *var);
extern bool map_insert(char *var, void *value, enum vartype type);
extern void clean_symtbl(void);

#endif
