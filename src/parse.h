#ifndef PARSER_H
#define PARSER_H

#include "cs165_api.h"
#include "message.h"

extern db_operator *parse_command(message *recv_message, message *send_message);

#endif
