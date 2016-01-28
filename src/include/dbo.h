#ifndef DBO_H
#define DBO_H

#include "cs165_api.h"

extern bool db_set_current(db *db);
extern struct db *get_curdb();
extern void cmd_sync(void);
extern db_operator *cmd_load(char *payload);
extern db_operator *cmd_create(size_t argc, const char **argv);
extern db_operator *cmd_rel_insert(size_t argc, const char **argv);
extern db_operator *cmd_select(size_t argc, const char **argv);
extern db_operator *cmd_fetch(size_t argc, const char **argv);
extern db_operator *cmd_tuple(size_t argc, const char **argv);
extern db_operator *cmd_add(size_t argc, const char **argv);
extern db_operator *cmd_avg(size_t argc, const char **argv);
extern db_operator *cmd_delete(size_t argc, const char **argv);
extern db_operator *cmd_rel_delete(size_t argc, const char **argv);
extern db_operator *cmd_join(size_t argc, const char **argv);
extern db_operator *cmd_max(size_t argc, const char **argv);
extern db_operator *cmd_min(size_t argc, const char **argv);
extern db_operator *cmd_sub(size_t argc, const char **argv);
extern db_operator *cmd_update(size_t argc, const char **argv);

#endif
