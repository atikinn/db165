#ifndef PARSER_H
#define PARSER_H

#include "message.h"
#include "cs165_api.h"

static const char EQUALS = '=';
static const char OPEN_PAREN = '(';
static const char CLOSE_PAREN = ')';
static const char COMMA = ',';
static const char NL = '\n';

static inline
int count_ch(const char *str, char ch) {
    int count = 0;
    for (; *str; count += (*str++ == ch)) ;
    return count;
}

extern db_operator *parse_command(message *recv_message, message *send_message);

#endif
