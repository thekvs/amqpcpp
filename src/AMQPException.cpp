#include "AMQPcpp.h"

namespace amqpcpp {

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
