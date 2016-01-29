#include <errno.h>
#include <stdlib.h>
#include <string.h>
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
    { "fetch", cmd_fetch },
    { "hashjoin", cmd_join },
    //{ "load", cmd_load },       // no response
    { "max", cmd_max },
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
const char **tokenize_args(char *msg, size_t *num_args) {
    char *args_start = strchr(msg, OPEN_PAREN);
    char *end = strchr(msg, CLOSE_PAREN);
    size_t argc = count_ch(args_start, COMMA) + 1 + 1; // extra 1 for cmd name

    *end = *args_start = '\0';

    char *eq = strchr(msg, EQUALS);
    char *cmd = (eq) ? eq + 1 : msg;

    char *comma = NULL;
    if (eq) {
        *eq = '\0';
        argc += (comma = strchr(msg, COMMA)) ? 2 : 1;   // one or two return vars;
        *eq = EQUALS;
    }

    const char **args_arr = malloc(argc * sizeof(void *) + 1);

    char *sep = ",";
    size_t i = 0;
    for (char *tmp, *arg = strtok_r(args_start + 1, sep, &tmp);
         arg;
         arg = strtok_r(NULL, sep, &tmp)) { args_arr[i++] = arg; }

    if (eq) {
        args_arr[i++] = msg;
        if (comma) {    // two return vars
            *comma = '\0';
            args_arr[i++] = comma + 1;
        }
        *eq = '\0';
    }

    args_arr[i++] = cmd;
    args_arr[i] = NULL;

    *num_args = argc;
    assert (argc == i);
    return args_arr;
}

/*
static inline size_t get_cmd_len(char *payload, char *open_paren) {
    char *eq = strchr(payload, EQUALS);
    return eq ? open_paren - (eq + 1) : open_paren - payload;
}
*/
static cmdptr get_cmdptr(const char *cmd) {
    for (size_t i = 0; i < sizeof command_map; i++)
        if (!strcasecmp(cmd, command_map[i].cmd))
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

    size_t argc;
    const char **argv = tokenize_args(recv_message->payload, &argc);
    for (size_t j = 0; j < argc; j++) {
        cs165_log(stderr, "%s ", argv[j]);
    }
    cs165_log(stderr, "\n");

    cmdptr fn = get_cmdptr(argv[argc-1]);

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

    if (dbo->type == TUPLE) {
        send_message->status = OK_WAIT_FOR_RESPONSE;
        send_message->payload_type = dbo->msgtype;
        send_message->count = dbo->tuple_count;
    } else {
        send_message->status = OK_DONE;
        send_message->count = 0;
    }

    return dbo;
}

