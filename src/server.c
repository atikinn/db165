/** server.c
 * CS165 Fall 2015
 *
 * This file provides a basic unix socket implementation for a server
 * used in an interactive client-server database.
 * The server should allow for multiple concurrent connections from clients.
 * Each client should be able to send messages containing queries to the
 * server.  When the server receives a message, it must:
 * 1. Respond with a status based on the query (OK, UNKNOWN_QUERY, etc.)
 * 2. Process any appropriate queries, if applicable.
 * 3. Return the query response to the client.
 *
 * For more information on unix sockets, refer to:
 * http://beej.us/guide/bgipc/output/html/multipage/unixsock.html
 **/
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/socket.h>
#include <unistd.h>
#include <string.h>
#include <strings.h>
#include <stdbool.h>
#include <assert.h>

#include "common.h"
#include "cs165_api.h"
#include "message.h"
#include "utils.h"
#include "sfhash.h"

static db_operator *cmd_load(int argc, const char **argv);
static db_operator *cmd_create(int argc, const char **argv);
static db_operator *cmd_sync(int argc, const char **argv);
static db_operator *cmd_insert(int argc, const char **argv);
static db_operator *cmd_rel_insert(int argc, const char **argv);
static db_operator *cmd_select(int argc, const char **argv);
static db_operator *cmd_fetch(int argc, const char **argv);
static db_operator *cmd_tuple(int argc, const char **argv);
static db_operator *cmd_add(int argc, const char **argv);
static db_operator *cmd_avg(int argc, const char **argv);
static db_operator *cmd_delete(int argc, const char **argv);
static db_operator *cmd_rel_delete(int argc, const char **argv);
static db_operator *cmd_hashjoin(int argc, const char **argv);
static db_operator *cmd_mergejoin(int argc, const char **argv);
static db_operator *cmd_max(int argc, const char **argv);
static db_operator *cmd_min(int argc, const char **argv);
static db_operator *cmd_sub(int argc, const char **argv);
static db_operator *cmd_update(int argc, const char **argv);

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
    { "insert", cmd_insert },
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

static db *curdb;

static const char EQUALS = '=';
static const char OPEN_PAREN = '(';
static const char CLOSE_PAREN = ')';
static const char COLON = ',';
static const char *DB = "db";
static const char *TBL = "tbl";
static const char *COL = "col";
static const char *IDX = "idx";

static int count_ch(const char *str, char ch) {
    int count = 0;
    for (; *str; count += (*str++ == ch)) ;
    return count;
}

static const char **tokenize_args(char *msg, int num_args) {
    char *start = strchr(msg, OPEN_PAREN) + 1;
    char *end = strchr(msg, CLOSE_PAREN);
    *end = '\0';
    const char **args_arr = malloc(num_args * sizeof(void *) + 1);
    int i = 0;
    for (char *tmp, *arg = strtok_r(start, &COLON, &tmp); arg;
         arg = strtok_r(NULL, &COLON, &tmp), i++) {
        args_arr[i] = arg;
    }
    args_arr[i] = NULL;
    return args_arr;
}

static size_t get_cmd_len(char *payload) {
    char *eq = strchr(payload, EQUALS);
    char *open_paren = strchr(payload, OPEN_PAREN);
    return eq ? open_paren - (eq + 1) : open_paren - payload;
}

static cmdptr get_cmdptr(char *req, size_t len) {
    for (size_t i = 0; i < sizeof command_map; i++)
        if (!strncasecmp(req, command_map[i].cmd, len))
            return command_map[i].fnptr;
    return NULL;
}

static db *select_db(const char *db_name) {
    (void)db_name;
    //TODO: get the db from vector of db
    return curdb;
}

const int VAR_MAP_SIZE = 1024;
static struct { size_t len; char *data; } *var_map[VAR_MAP_SIZE];

static char *map_get(const char *var) {
    size_t idx = super_fast_hash(var, strlen(var));
    //var_map[idx]
    return NULL;
}

static db_operator *cmd_create_db(const char *db_name) {
    db_operator *dbo = malloc(sizeof *dbo);
    if (dbo == NULL) return NULL;
    dbo->type = CREATE_DB;
    dbo->create_name = strdup(db_name);
    return dbo;
}

static db_operator *cmd_create_tbl(const char *db_name, const char *tbl_name, int size) {
    db_operator *dbo = malloc(sizeof *dbo);
    if (dbo == NULL) return NULL;
    dbo->type = CREATE_TBL;
    dbo->create_name = strdup(tbl_name);
    dbo->db = select_db(db_name);
    dbo->table_size = size;
    return dbo;
}

static bool should_be_sorted(const char *str) {
    if (!strcmp(str, "sorted")) return true;
    if (!strcmp(str, "unsorted")) return false;
    return false;
}

static db_operator *cmd_create_column(table *tbl, const char *col_name, const char *sorted) {
    db_operator *dbo = malloc(sizeof *dbo);
    if (dbo == NULL) return NULL;
    dbo->type = CREATE_COL;
    dbo->create_name = strdup(col_name);
    dbo->sorted = should_be_sorted(sorted);
    dbo->tables = tbl;
    return dbo;
}

static db_operator *cmd_rel_insert(int argc, const char **argv) {
    db_operator *dbo = malloc(sizeof *dbo);
    if (dbo == NULL) return NULL;
    dbo->type = INSERT;
    dbo->value1 = malloc(sizeof(int) * argc-1);
    dbo->tables = map_get(argv[0]);
    assert(dbo->value1);
    for (int i = 1; i < argc; i++)
        dbo->value1[i] = strtol(argv[i], NULL, 10);
    return dbo;
}

static OperatorType get_create_type(const char *type) {
    if (!strcmp(type, DB)) return CREATE_DB;
    if (!strcmp(type, TBL)) return CREATE_TBL;
    if (!strcmp(type, COL)) return CREATE_COL;
    if (!strcmp(type, IDX)) return CREATE_IDX;
    return INVALID;
}

static db_operator *cmd_create(int argc, const char **argv) {
    (void)argc;
    table *tbl;
    switch(get_create_type(argv[0])) {
        case CREATE_DB:
            return cmd_create_db(argv[0]);
        case CREATE_TBL:
            return cmd_create_tbl(argv[2], argv[1], strtol(argv[3], NULL, 10));
        case CREATE_COL:
            tbl = map_get(argv[2]);
            return cmd_create_column(tbl, argv[1], argv[3]);
        case CREATE_IDX: break;
        default: break;
    }
    return NULL;
}

static db_operator *cmd_tuple(int argc, const char **argv) {
    (void)argc;
    (void)argv;
    return NULL;
}
static db_operator *cmd_select(int argc, const char **argv) {
    (void)argc;
    (void)argv;
    return NULL;
}
static db_operator *cmd_fetch(int argc, const char **argv) {
    (void)argc;
    (void)argv;
    return NULL;
}

/* Execution */
/*
status create_table(db* db, const char* name, size_t num_columns, table** table) {
    *table = malloc(sizeof **table);
    if (*table == NULL) {
        status s = { ERROR, "out of memory" };
        return s;
    }
    (*table)->name = strdup(name);
    (*table)->col_count = num_columns;
    (*table)->col = NULL;
    (*table)->length = 0;
    // TODO: factor out into a separate function db_add_table
    db->tables = table;
    db->table_count++;
    status s = { OK, NULL };
    return s;
}

status create_column(table *table, const char* name, column** col) {
    *col = malloc(sizeof **col);
    if (*col == NULL) {
        status s = { ERROR, "out of memory" };
        return s;
    }
    (*col)->name = strdup(name);
    (*col)->data = NULL;
    (*col)->index = NULL;
    // TODO: factor out into a separate function table_add_col
    table->col_count++;
    table->col = col;

    status s = { OK, NULL };
    return s;
}

status create_db(const char *db_name, db **db) {
    *db = malloc(sizeof **db);
    if (*db == NULL) {
        status s = { ERROR, "out of memory" };
        return s;
    }
    (*db)->name = strdup(db_name);
    (*db)->table_count = 0;
    (*db)->tables = NULL;
    status s = { OK, NULL };
    db_set_current(*db);
    return s;
}

static void db_set_current(db *db) {
    curdb = db;
    return;
}
*/

/**
 * parse_command takes as input the send_message from the client and then
 * parses it into the appropriate query. Stores into send_message the
 * status to send back.
 * Returns a db_operator.
 **/
db_operator *parse_command(message *recv_message, message *send_message) {
    cs165_log(stdout, recv_message->payload);

    db_operator *dbo = NULL;
    int argc = count_ch(recv_message->payload, COLON) + 1;
    const char **argv = tokenize_args(recv_message->payload, argc);
    size_t len = get_cmd_len(recv_message->payload);
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

/* TODO delete later
static char *get_send_msg(const char *cmd, message_status status) {
    switch(status) {
        case OK_WAIT_FOR_RESPONSE:
            if (!strcmp(cmd, "create")) return "created";
            if (!strcmp(cmd, "load")) return "loaded";
            if (!strcmp(cmd, "sync")) return "synched";
            if (!strcmp(cmd, "tuple")) return "TODO";
            return "";
        case UNKNOWN_COMMAND: return "unknown";
        case INCORRECT_FORMAT: return "incorrect";
        case OK_DONE: return "done";
    }
}
*/
    /*
    for (const char *arg = *argv; arg; arg = *(++argv))
        cs165_log(stderr, "%s\n", arg);
    */

/** execute_db_operator takes as input the db_operator and executes the query.
 * It should return the result (currently as a char*, although I'm not clear
 * on what the return type should be, maybe a result struct, and then have
 * a serialization into a string message).
 **/
char *execute_db_operator(db_operator *query) {
    free(query);
    return "created_db";
}

/**
 * handle_client(client_socket)
 * This is the execution routine after a client has connected.
 * It will continually listen for messages from the client and execute queries.
 **/
void handle_client(int client_socket) {
    int done = 0;
    int length = 0;

    log_info("Connected to socket: %d.\n", client_socket);

    // Create two messages, one from which to read and one from which to receive
    message send_message = {0, 0, 0};
    message recv_message;

    // Continually receive messages from client and execute queries.
    // 1. Parse the command
    // 2. Handle request if appropriate
    // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
    // 4. Send response of request.
    do {
        length = recv(client_socket, &recv_message, sizeof(message), 0);
        if (length < 0) {
            log_err("Client connection closed!\n");
            exit(1);
        } else if (length == 0) {
            done = 1;
        }

        if (!done) {
            char recv_buffer[recv_message.length];
            length = recv(client_socket, recv_buffer, recv_message.length, 0);
            recv_message.payload = recv_buffer;
            recv_message.payload[recv_message.length] = '\0';

            // 1. Parse command
            db_operator *query = parse_command(&recv_message, &send_message);

            // 2. Handle request
            char* result = execute_db_operator(query);
            send_message.length = strlen(result);

            // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
            if (send(client_socket, &(send_message), sizeof(message), 0) == -1) {
                log_err("Failed to send message.");
                exit(1);
            }

            // 4. Send response of request
            if (send(client_socket, result, send_message.length, 0) == -1) {
                log_err("Failed to send message.");
                exit(1);
            }
        }
    } while (!done);

    log_info("Connection closed at socket %d!\n", client_socket);
    close(client_socket);
}

/**
 * setup_server()
 *
 * This sets up the connection on the server side using unix sockets.
 * Returns a valid server socket fd on success, else -1 on failure.
 **/
int setup_server() {
    int server_socket;
    size_t len;
    struct sockaddr_un local;

    log_info("Attempting to setup server...\n");

    if ((server_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        log_err("L%d: Failed to create socket.\n", __LINE__);
        return -1;
    }

    local.sun_family = AF_UNIX;
    strncpy(local.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
    unlink(local.sun_path);

    /*
    int on = 1;
    if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, (char *)&on, sizeof(on)) < 0)
    {
        log_err("L%d: Failed to set socket as reusable.\n", __LINE__);
        return -1;
    }
    */

    len = strlen(local.sun_path) + sizeof(local.sun_family) + 1;
    if (bind(server_socket, (struct sockaddr *)&local, len) == -1) {
        log_err("L%d: Socket failed to bind.\n", __LINE__);
        return -1;
    }

    if (listen(server_socket, 5) == -1) {
        log_err("L%d: Failed to listen on socket.\n", __LINE__);
        return -1;
    }

    return server_socket;
}

// Currently this main will setup the socket and accept a single client.
// After handling the client, it will exit.
// You will need to extend this to handle multiple concurrent clients
// and remain running until it receives a shut-down command.
int main(void)
{
    int server_socket = setup_server();
    if (server_socket < 0) {
        exit(1);
    }

    log_info("Waiting for a connection %d ...\n", 10);

    struct sockaddr_un remote;
    socklen_t t = sizeof(remote);
    int client_socket = 0;

    if ((client_socket = accept(server_socket, (struct sockaddr *)&remote, &t)) == -1) {
        log_err("L%d: Failed to accept a new connection.\n", __LINE__);
        exit(1);
    }

    handle_client(client_socket);

    return 0;
}

static db_operator *cmd_avg(int argc, const char **argv) {
    (void)argc;
    (void)argv;
    return NULL;
}
static db_operator *cmd_delete(int argc, const char **argv) {
    (void)argc;
    (void)argv;
    return NULL;
}
static db_operator *cmd_min(int argc, const char **argv) {
    (void)argc;
    (void)argv;
    return NULL;
}
static db_operator *cmd_max(int argc, const char **argv) {
    (void)argc;
    (void)argv;
    return NULL;
}
static db_operator *cmd_load(int argc, const char **argv) {
    (void)argc;
    (void)argv;
    return NULL;
}
static db_operator *cmd_sync(int argc, const char **argv) {
    (void)argc;
    (void)argv;
    return NULL;
}
static db_operator *cmd_hashjoin(int argc, const char **argv) {
    (void)argc;
    (void)argv;
    return NULL;
}
static db_operator *cmd_mergejoin(int argc, const char **argv) {
    (void)argc;
    (void)argv;
    return NULL;
}
static db_operator *cmd_insert(int argc, const char **argv) {
    (void)argc;
    (void)argv;
    return NULL;
}
static db_operator *cmd_rel_delete(int argc, const char **argv) {
    (void)argc;
    (void)argv;
    return NULL;
}
static db_operator *cmd_sub(int argc, const char **argv) {
    (void)argc;
    (void)argv;
    return NULL;
}
static db_operator *cmd_update(int argc, const char **argv) {
    (void)argc;
    (void)argv;
    return NULL;
}
static db_operator *cmd_add(int argc, const char **argv) {
    (void)argc;
    (void)argv;
    return NULL;
}
