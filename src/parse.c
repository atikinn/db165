#include <errno.h>
#include <stdlib.h>
#include <strings.h>
#include <assert.h>

#include "common.h"
#include "cs165_api.h"
#include "message.h"
#include "utils.h"
#include "dbo.h"
#include "parse.h"

const char EQUALS = '=';
const char OPEN_PAREN = '(';
const char CLOSE_PAREN = ')';
const char COMMA = ',';

typedef db_operator *(*cmdptr)(int, const char**);
static struct {
    const char *cmd;
    cmdptr fnptr;
} command_map[] = {
    { "add", cmd_add },
    { "avg", cmd_avg },
    { "create", cmd_create },
    { "delete", cmd_delete },
    { "fetch", cmd_fetch },
    { "hashjoin", cmd_hashjoin },
    { "load", cmd_load },
    { "max", cmd_max },
    { "mergejoin", cmd_mergejoin },
    { "min", cmd_min },
    { "relational_delete", cmd_rel_delete },
    { "relational_insert", cmd_rel_insert },
    { "select", cmd_select },
    { "sub", cmd_sub },
    { "sync", cmd_sync },
    { "tuple", cmd_tuple },
    { "update", cmd_update },
    { NULL, NULL }
};

static inline int count_ch(const char *str, char ch) {
    int count = 0;
    for (; *str; count += (*str++ == ch)) ;
    return count;
}

static const char **tokenize_args(char *msg, int num_args) {
    char *eq = strchr(msg, EQUALS);
    char *comma = NULL;
    int i = 0;
    if (eq) {
        *eq = '\0';
        i = (comma = strchr(msg, COMMA)) ? 2 : 1;
        num_args += i;
        *eq = EQUALS;
    }

    const char **args_arr = malloc(num_args * sizeof(void *) + 1);

    char *start = strchr(msg, OPEN_PAREN) + 1;
    char *end = strchr(msg, CLOSE_PAREN);
    *end = '\0';
    for (char *tmp, *arg = strtok_r(start, &COMMA, &tmp); arg;
         arg = strtok_r(NULL, &COMMA, &tmp)) {
        args_arr[i++] = arg;
    }

    if (eq) {
        args_arr[0] = msg;
        if (comma) {
            *comma = '\0';
            args_arr[1] = comma + 1;
        }
        *eq = '\0';
    }

    args_arr[i] = NULL;
    return args_arr;
}

static inline size_t get_cmd_len(char *payload, char *open_paren) {
    char *eq = strchr(payload, EQUALS);
    return eq ? open_paren - (eq + 1) : open_paren - payload;
}

static cmdptr get_cmdptr(char *req, size_t len) {
    for (size_t i = 0; i < sizeof command_map; i++)
        if (!strncasecmp(req, command_map[i].cmd, len))
            return command_map[i].fnptr;
    return NULL;
}

/**
 * parse_command takes as input the send_message from the client and then
 * parses it into the appropriate query. Stores into send_message the
 * status to send back.
 * Returns a db_operator.
 **/
//status query_prepare(const char* query, db_operator** op);
db_operator *parse_command(message *recv_message, message *send_message) {
    cs165_log(stdout, recv_message->payload);

    db_operator *dbo = NULL;
    char *args_start = strchr(recv_message->payload, OPEN_PAREN);
    int argc = count_ch(args_start, COMMA) + 1;
    const char **argv = tokenize_args(recv_message->payload, argc);
    size_t len = get_cmd_len(recv_message->payload, args_start);
    cmdptr fn = get_cmdptr(recv_message->payload, len);
    if (!fn) {  // should never happen
        send_message->status = UNKNOWN_COMMAND;
        goto out;
    }
    dbo = fn(argc, argv);
    if (dbo == NULL) { // should never happen
        send_message->status = INTERNAL_ERROR;
        goto out;
    }
    send_message->status = OK_WAIT_FOR_RESPONSE;
out:
    free(argv);
    return dbo;
}

