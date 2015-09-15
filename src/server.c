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

#include "common.h"
#include "cs165_api.h"
#include "message.h"
#include "utils.h"

typedef int (*cmdptr)(int argc, const char *argv[]);

struct command {
    const char *command;
    const cmdptr funptr;
};

int cmd_load(int argc, const char *argv[]);
int cmd_create(int argc, const char *argv[]);
int cmd_sync(int argc, const char *argv[]);
int cmd_insert(int argc, const char *argv[]);
int cmd_rel_insert(int argc, const char *argv[]);
int cmd_select(int argc, const char *argv[]);
int cmd_fetch(int argc, const char *argv[]);

status open_db(const char* filename, db** db, OpenFlags flags); // load
status create_db(const char* db_name, db** db); // create
status create_table(db* db, const char* name, size_t num_columns, table** table); // tb = create
status create_column(table *table, const char* name, column** col); // col = create
status insert(column *col, int data); // insert
status col_scan(comparator *f, column *col, result **r);
// tuple

status drop_db(db* db); // drop_db
status sync_db(db* db); // syng
status drop_table(db* db, table* table); // drop_table
status create_index(column* col, IndexType type); // create
status delete(column *col, int *pos);   // delete
status update(column *col, int *pos, int new_val); // update
status index_scan(comparator *f, column *col, result **r);

static const char *commands[] = { "create", "load", "sync", "tuple" };

static const char *ops[] = {
    "add", "avg", "delete", "fetch", "hashjoin", "insert", "max", "mergejoin",
    "min", "relational_delete", "relational_insert", "select", "sub","update"
};

static db *cur_db;

static const char EQUALS = '=';
static const char OPEN_PAREN = '(';
static const char CLOSE_PAREN = ')';

static message_status check_command(char *req, size_t len) {
    for (size_t i = 0; commands[i]; i++)
        if (!strncasecmp(req, commands[i], len))
            return OK_DONE;
    return OK_WAIT_FOR_RESPONSE;
}

static message_status parse_request(char *req) {
    char *open_paren = strchr(req, OPEN_PAREN);
    char *eq = strchr(req, EQUALS);
    char *start = req;
    size_t len;
    if (eq) {
        start = eq + 1;
        len = open_paren - start;
    } else {
        len = open_paren - start;
    }
    return OK_DONE;
}

static void fill_message(message_t *msg, message_status st, char *error) {
    msg->status = st;
    snprintf(msg->message, sizeof(DEFAULT_MESSAGE_BUFFER_SIZE), "%s", error);
}

/**
 * parse_command takes as input the send_message from the client and then
 * parses it into the appropriate query. Stores into send_message the
 * status to send back.
 * Returns a db_operator.
 **/
db_operator *parse_command(message_t *recv_message, message_t *send_message) {
    //send_message->status = OK_WAIT_FOR_RESPONSE;
    db_operator *dbo = malloc(sizeof(db_operator));

    message_status st = parse_request(recv_message->message);

    // TODO: load, create, tuple are not db opearators
    // TODO: relational_insert, select, fetch is db operator


//open_db(const char* filename, db** db, OpenFlags flags); load
//create_db(const char* db_name, db** db); create
//create_table(db* db, const char* name, size_t num_columns, table** table); tb = create
//create_column(table *table, const char* name, column** col); col = create

    // fill in the proper db_operator fields for now we just log the message
    cs165_log(stdout, recv_message->message);

    return dbo;

err:
    free(dbo);
    return NULL;
}

/** execute_db_operator takes as input the db_operator and executes the query.
 * It should return the result (currently as a char*, although I'm not clear
 * on what the return type should be, maybe a result struct, and then have
 * a serialization into a string message).
 **/
char *execute_db_operator(db_operator *query) {
    free(query);
    return "165";
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
    message_t send_message;
    message_t recv_message;

    // Continually receive messages from client and execute queries.
    // 1. Parse the command
    // 2. Send status of the received message (OK, UNKNOWN_QUERY, etc)
    // 3. Handle request if appropriate
    // 4. Send response of request.
    do {
        length = recv(client_socket, &recv_message, sizeof(message_t), 0);
        if (length < 0) {
            log_err("Client connection closed!\n");
            exit(1);
        } else if (length == 0) {
            done = 1;
        }

        if (!done) {
            // 1. Parse command
            db_operator *query = parse_command(&recv_message, &send_message);

            // 2. Send status of the received message (OK, UNKNOWN_QUERY, etc)
            if (send(client_socket, &(send_message), sizeof(message_t), 0) == -1) {
                log_err("Failed to send message.");
                exit(1);
            }

            // 3. Handle request
            char *result = execute_db_operator(query);
            int response_length = strlen(result) < DEFAULT_MESSAGE_BUFFER_SIZE
                                    ? strlen(result)
                                    : DEFAULT_MESSAGE_BUFFER_SIZE;
            strncpy(send_message.message, result, response_length);
            send_message.message[response_length] = '\0';

            // 4. Send response of request
            if (send(client_socket, &(send_message), sizeof(message_t), 0) == -1) {
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

int main(void) {
    int server_socket = setup_server();
    if (server_socket < 0) {
        exit(1);
    }

    log_info("Waiting for a connection %d ...\n", 10);

    struct sockaddr_un remote;
    socklen_t t = sizeof(remote);
    int client_socket = 0;

    // For now, only accept a single client, although later we can put this in a loop
    // to accept for multiple clients.
    if ((client_socket = accept(server_socket, (struct sockaddr *)&remote, &t)) == -1) {
        log_err("L%d: Failed to accept a new connection.\n", __LINE__);
        exit(1);
    }

    handle_client(client_socket);

    return 0;
}

