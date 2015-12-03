#include <errno.h>
#include <stdlib.h>
#include <strings.h>
#include <assert.h>

#include "message.h"
#include "common.h"
#include "cs165_api.h"
#include "utils.h"
#include "dbo.h"
#include "parse.h"
#include "sync.h"

typedef db_operator *(*cmdptr)(size_t, const char**);
static struct {
    const char *cmd;
    cmdptr fnptr;
} command_map[] = {
    { "add", cmd_add },
    { "avg", cmd_avg },
    { "create", cmd_create },   // no response
    { "delete", cmd_delete },   // no response
    { "fetch", cmd_fetch },
    { "hashjoin", cmd_hashjoin },
    //{ "load", cmd_load },       // no response
    { "max", cmd_max },
    { "mergejoin", cmd_mergejoin },
    { "min", cmd_min },
    { "relational_delete", cmd_rel_delete },    // no response
    { "relational_insert", cmd_rel_insert },    // no response
    { "select", cmd_select },
    { "sub", cmd_sub },
    //{ "sync", cmd_sync },                       // no response
    { "tuple", cmd_tuple },
    { "update", cmd_update },                   // no response
    { NULL, NULL }
};

static
const char **tokenize_args(char *msg, int num_args) {
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
    char *sep = ",";
    for (char *tmp, *arg = strtok_r(start, sep, &tmp);
         arg;
         arg = strtok_r(NULL, sep, &tmp)) { args_arr[i++] = arg; }

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
    db_operator *dbo = NULL;

    if (recv_message->status == STRDATA) {  // bulk load
        dbo = cmd_load(recv_message->payload);
        send_message->status = OK_DONE;
        return dbo;
    }

    cs165_log(stdout, recv_message->payload);
    if (strncmp(recv_message->payload, "--", 2) == 0) {
        send_message->status = OK_DONE;
        return NULL;
    }

    if (strncmp(recv_message->payload, "shutdown", 8) == 0) {
        send_message->status = OK_DONE;
        sync();
        return NULL;
    }

    char *args_start = strchr(recv_message->payload, OPEN_PAREN);
    int argc = count_ch(args_start, COMMA) + 1;
    const char **argv = tokenize_args(recv_message->payload, argc);
    size_t len = get_cmd_len(recv_message->payload, args_start);
    cmdptr fn = get_cmdptr(recv_message->payload, len);

    if (!fn) {  // should never happen
        send_message->status = UNKNOWN_COMMAND;
        free(argv);
        return dbo;
    }

    dbo = fn(argc, argv);
    free(argv);

    if (dbo == NULL) { // should never happen at this point
        send_message->status = INTERNAL_ERROR;
        return dbo;
    }

    send_message->status = OK_WAIT_FOR_RESPONSE;
    return dbo;
}

