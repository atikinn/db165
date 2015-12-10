#define _XOPEN_SOURCE
/**
 * client.c
 *  CS165 Fall 2015
 *
 * This file provides a basic unix socket implementation for a client
 * used in an interactive client-server database.
 * The client receives input from stdin and sends it to the server.
 * No pre-processing is done on the client-side.
 *
 * For more information on unix sockets, refer to:
 * http://beej.us/guide/bgipc/output/html/multipage/unixsock.html
 **/
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <stdbool.h>
#include <fcntl.h>
#include <errno.h>

#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>

#include "common.h"
#include "message.h"
#include "utils.h"

#define DEFAULT_STDIN_BUFFER_SIZE 1024
void get_all_buf(int sock, char *output) {
    (void) output;
    char buffer[1024];

    int n;
    while((errno = 0, (n = recv(sock, buffer, sizeof(buffer), 0)) > 0)
            || errno == EINTR) {
        if(n > 0)
            ;
            //output.append(buffer, n);
    }

    if(n < 0) {
        /* handle error - for example throw an exception*/
    }
}

char *itoa (int value, char *result, int base) {
    // check that the base if valid
    if (base < 2 || base > 36) { *result = '\0'; return result; }

    char* ptr = result, *ptr1 = result, tmp_char;
    int tmp_value;

    do {
        tmp_value = value;
        value /= base;
        *ptr++ = "zyxwvutsrqponmlkjihgfedcba9876543210123456789abcdefghijklmnopqrstuvwxyz" [35 + (tmp_value - value * base)];
    } while (value);

    // Apply negative sign
    if (tmp_value < 0) *ptr++ = '-';
    *ptr-- = '\0';
    while (ptr1 < ptr) {
        tmp_char = *ptr;
        *ptr--= *ptr1;
        *ptr1++ = tmp_char;
    }
    return result;
}

/**
 * connect_client()
 *
 * This sets up the connection on the client side using unix sockets.
 * Returns a valid client socket fd on success, else -1 on failure.
 *
 **/
int connect_client() {
    int client_socket;
    size_t len;
    struct sockaddr_un remote;

    log_info("Attempting to connect...\n");

    if ((client_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        log_err("L%d: Failed to create socket.\n", __LINE__);
        return -1;
    }

    remote.sun_family = AF_UNIX;
    strncpy(remote.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
    len = strlen(remote.sun_path) + sizeof(remote.sun_family) + 1;
    if (connect(client_socket, (struct sockaddr *)&remote, len) == -1) {
        perror("client connect failed: ");
        return -1;
    }

    log_info("Client connected at socket: %d.\n", client_socket);
    return client_socket;
}

void send_file(int client_socket, message *send_message) {
    char *openq = strchr(send_message->payload, '"');
    char *closeq = strchr(openq + 1, '"');
    int flen = closeq - openq;
    char filename[flen];
    filename[flen-1] = '\0';
    strncpy(filename, openq+1, flen-1);

    int fd = open(filename, O_RDONLY);
    if (fd == -1) {
        log_info("failed open the file\n");
        exit(1);
    }

    struct stat s;
    fstat(fd, &s);
    send_message->length = s.st_size;
    send_message->payload = mmap(NULL, s.st_size, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);

    int len = 0;
    message recv_message;

    if (send_message->length > 1) {
        //log_info("FILENAME: %s of len %d\n", filename, s.st_size);
        // Send the message_header, which tells server payload size
        if (send(client_socket, send_message, sizeof(message), 0) == -1) {
            log_err("Failed to send message header.");
            exit(1);
        }

        // Send the payload (file) to server
        int r = send(client_socket, send_message->payload, send_message->length, 0);
        if (r == -1) {
            log_err("Failed to send file.");
            exit(1);
        }

        if (r != send_message->length) {
            log_err("actually sent: %d\n", r);
            exit(1);
        }

        // Always wait for server response (even if it is just an OK message)
        if ((len = recv(client_socket, &recv_message, sizeof(message), 0)) > 0) {
            if (recv_message.status == OK_WAIT_FOR_RESPONSE
                && (int) recv_message.length > 0) {
                // Calculate number of bytes in response package
                int num_bytes = (int) recv_message.length;
                char payload[num_bytes + 1];

                // Receive the payload and print it out
                if ((len = recv(client_socket, payload, num_bytes, 0)) > 0) {
                    payload[num_bytes] = '\0';
                    printf("%s\n", payload);
                }
            }
        } else {
            if (len < 0) {
                log_err("Failed to receive message.");
            } else {
                log_info("Server closed connection\n");
            }
            exit(1);
        }
    }

    munmap(send_message->payload, send_message->length);
}

void send_command(int client_socket, message *send_message) {
    // Only process input that is greater than 1 character.
    // Ignore things such as new lines.
    // Otherwise, convert to message and send the message and the
    // payload directly to the server.
    int len = 0;
    message recv_message;

    //log_info("COMMAND: %s of len %d\n", send_message->payload, send_message->length);

    if (send_message->length > 1) {
        // Send the message_header, which tells server payload size
        if (send(client_socket, send_message, sizeof(message), 0) == -1) {
            log_err("Failed to send message header.");
            exit(1);
        }

        // Send the payload (query) to server
        if (send(client_socket, send_message->payload, send_message->length, 0) == -1) {
            log_err("Failed to send query payload.");
            exit(1);
        }

        // Always wait for server response (even if it is just an OK message)
        if ((len = recv(client_socket, &recv_message, sizeof(message), 0)) > 0) {
            int num_bytes = (int) recv_message.length;
            //log_info("num_bytes = %d, status = %d\n", num_bytes, recv_message.status);
            if (recv_message.status == OK_WAIT_FOR_RESPONSE && num_bytes > 0) {
                // Calculate number of bytes in response package
                char payload[num_bytes + 1];
                payload[num_bytes] = '\0';
                // Receive the payload and print it out
                // TODO two buffers, one local, another full or just print out
                if ((len = recv(client_socket, payload, num_bytes, 0)) > 0) {
                    if (recv_message.payload_type == DOUBLE_VAL) {
                        long double *vals = (double long *)payload;
                        for (size_t i = 0; i < num_bytes / sizeof(long double); i++)
                            fprintf(stderr, "%Lf\n", vals[i]);
                    } else if (recv_message.payload_type == VECTOR ||
                               recv_message.payload_type == INT_VAL) {
                        int *vals = (int *)payload;
                        for (size_t i = 0; i < num_bytes / sizeof(int); i++)
                            fprintf(stderr, "%d\n", vals[i]);
                    } else if (recv_message.payload_type == LONG_VECTOR) {
                        long int *vals = (long int *)payload;
                        for (size_t i = 0; i < num_bytes / sizeof(long int); i++)
                            fprintf(stderr, "%ld\n", vals[i]);
                    } else {
                        log_err("received unknown payload response\n");
                        exit(1);
                    }
                }
            }
        } else {
            if (len < 0) {
                log_err("Failed to receive message.");
            } else {
                log_info("Server closed connection\n");
            }
            exit(1);
        }
    }
}

const char *load = "load";

int main(void) {
    int client_socket = connect_client();
    if (client_socket < 0) {
        exit(1);
    }

    message send_message;

    // Always output an interactive marker at the start of each command if the
    // input is from stdin. Do not output if piped in from file or from other fd
    char* prefix = "";
    if (isatty(fileno(stdin))) {
        prefix = "db_client > ";
    }

    char *output_str = NULL;
    // Continuously loop and wait for input. At each iteration:
    // 1. output interactive marker
    // 2. read from stdin until eof.
    char read_buffer[DEFAULT_STDIN_BUFFER_SIZE];

    while (printf("%s", prefix),
           output_str = fgets(read_buffer, DEFAULT_STDIN_BUFFER_SIZE, stdin),
           !feof(stdin)) {

        if (output_str == NULL) {
            log_err("fgets failed.\n");
            break;
        }

        log_info("%s", output_str);
        if (strncmp(read_buffer, load, strlen(load)) == 0) {
            send_message.status = STRDATA;
            send_file(client_socket, &send_message);
        } else {
            send_message.payload = read_buffer;
            send_message.length = strlen(read_buffer);
            send_message.status = COMMAND;
            send_command(client_socket, &send_message);
        }
    }

    close(client_socket);
    return 0;
}
