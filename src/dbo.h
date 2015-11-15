#ifndef DBO_H
#define DBO_H

#include "cs165_api.h"

extern void db_set_current(db *db);
extern db_operator *cmd_load(int argc, const char **argv);
extern db_operator *cmd_create(int argc, const char **argv);
extern db_operator *cmd_sync(int argc, const char **argv);
extern db_operator *cmd_insert(int argc, const char **argv);
extern db_operator *cmd_rel_insert(int argc, const char **argv);
extern db_operator *cmd_select(int argc, const char **argv);
extern db_operator *cmd_fetch(int argc, const char **argv);
extern db_operator *cmd_tuple(int argc, const char **argv);
extern db_operator *cmd_add(int argc, const char **argv);
extern db_operator *cmd_avg(int argc, const char **argv);
extern db_operator *cmd_delete(int argc, const char **argv);
extern db_operator *cmd_rel_delete(int argc, const char **argv);
extern db_operator *cmd_hashjoin(int argc, const char **argv);
extern db_operator *cmd_mergejoin(int argc, const char **argv);
extern db_operator *cmd_max(int argc, const char **argv);
extern db_operator *cmd_min(int argc, const char **argv);
extern db_operator *cmd_sub(int argc, const char **argv);
extern db_operator *cmd_update(int argc, const char **argv);

#endif
