#include "AMQPcpp.h"

namespace amqpcpp {

AMQPException::AMQPException(const char *file, int line,
    amqp_rpc_reply_t *res)
{
    if (res->reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION) {
        snprintf(buff, sizeof(buff), "%s",
            res->library_error ? strerror(res->library_error) : "end-of-stream");
    } else {
        if (res->reply_type == AMQP_RESPONSE_SERVER_EXCEPTION) {
            if (res->reply.id == AMQP_CONNECTION_CLOSE_METHOD) {
                amqp_connection_close_t *m =
                    (amqp_connection_close_t *)res->reply.decoded;
                snprintf(
                    buff, sizeof(buff),
                    "server connection error %d, message: %.*s",
                    m->reply_code, (int)m->reply_text.len,
                    (char *)m->reply_text.bytes
                );
            } else if (res->reply.id == AMQP_CHANNEL_CLOSE_METHOD) {
                amqp_channel_close_t *n =
                    (amqp_channel_close_t *)res->reply.decoded;
                snprintf(
                    buff, sizeof(buff),
                    "server channel error %d, message: %.*s class=%d method=%d",
                    n->reply_code, (int)n->reply_text.len, (char *)n->reply_text.bytes,
                    (int)n->class_id, n->method_id
                );
            } else {
                snprintf(buff, sizeof(buff),
                    "unknown server error, method id 0x%08X", res->reply.id);
            }
        }
    }

    append_position(file, line);
}

AMQPException::AMQPException(const char *file, int line,
    const char *fmt, ...): msg(NULL)
{
    va_list ap;

    va_start(ap, fmt);
    vsnprintf(buff, sizeof(buff), fmt, ap);
    va_end(ap);

    append_position(file, line);
}

void
AMQPException::append_position(const char *file, int line)
{
    size_t len;

    len = 0;
    memset(line_str, 0, sizeof(line_str));
    snprintf(line_str, sizeof(line_str), "%i", line);
    len = strlen(buff) + 5 /* ' (at ' */ + strlen(file) + 1 /* ':' */ +
        strlen(line_str) + 1 /* ')' */ + 1 /* terminating zero */;
    msg = static_cast<char*>(calloc(1, len));
    snprintf(msg, len, "%s (at %s:%s)", buff, sanitize_file_name(file),
        line_str);
}

const char*
AMQPException::sanitize_file_name(const char *file)
{
    const char *s;
    char       *p;

    p = const_cast<char*>(strrchr(file, '/'));

    if (p == NULL) {
        s = file;
    } else {
        p++;
        s = p;
    }

    return s;
}

} // namespace
