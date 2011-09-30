#ifndef __AMQPCPP
#define __AMQPCPP

#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <cstdarg>

#include <unistd.h>
#include <stdint.h>
#include <inttypes.h>

#include <iostream>
#include <vector>
#include <string>
#include <map>
#include <memory>
#include <exception>

#include "amqp.h"
#include "amqp_framing.h"

namespace amqpcpp {

const int HEADER_FOOTER_SIZE = 8;      //  7 bytes up front, then payload, then 1 byte footer
const int FRAME_MAX          = 131072; // max lenght (size) of frame

enum {
    AMQP_AUTODELETE     = 1,
    AMQP_DURABLE        = 2,
    AMQP_PASSIVE        = 4,
    AMQP_MANDATORY      = 8,
    AMQP_IMMIDIATE      = 16,
    AMQP_IFUNUSED       = 32,
    AMQP_EXCLUSIVE      = 64,
    AMQP_NOWAIT         = 128,
    AMQP_NOACK          = 256,
    AMQP_NOLOCAL        = 512,
    AMQP_MULTIPLE       = 1024
};

class AMQPQueue;

enum AMQPEvents_e {
    AMQP_MESSAGE,
    AMQP_SIGUSR,
    AMQP_CANCEL,
    AMQP_CLOSE_CHANNEL
};

class AMQPException: public std::exception {
public:

    AMQPException(const char *file, int line, const char *fmt, ...);

    virtual ~AMQPException() throw() {
        free(msg);
    };

    virtual const char* what() const throw() {
        return (msg == NULL ? "" : msg);
    }

private:

    char  buff[1024];
    char *msg;
    char  line_str[sizeof("2147483647")];

    const char* sanitize_file_name(const char *file);
    void append_position(const char *file, int line);
};

#define THROW_AMQP_EXC(args...)  (throw AMQPException(__FILE__, __LINE__, args))

#define THROW_AMQP_EXC_IF_FAILED(status, context) do {                        \
    switch (status.reply_type) {                                              \
    case AMQP_RESPONSE_NORMAL:                                                \
        break;                                                                \
    case AMQP_RESPONSE_NONE:                                                  \
        throw AMQPException(__FILE__, __LINE__,                               \
            "%s: missing RPC reply type!", context);                          \
    case AMQP_RESPONSE_LIBRARY_EXCEPTION:                                     \
        throw AMQPException(__FILE__, __LINE__,                               \
            "%s: %s", context, amqp_error_string(status.library_error));      \
    case AMQP_RESPONSE_SERVER_EXCEPTION:                                      \
        switch (status.reply.id) {                                            \
        case AMQP_CONNECTION_CLOSE_METHOD:                                    \
            {                                                                 \
                 amqp_connection_close_t *m =                                 \
                     static_cast<amqp_connection_close_t*>(                   \
                         status.reply.decoded);                               \
                 throw AMQPException(__FILE__, __LINE__,                      \
                     "%s: server connection error %d, message: %.*s",         \
                     context,                                                 \
                     m->reply_code,                                           \
                     static_cast<int>(m->reply_text.len),                     \
                     static_cast<char*>(m->reply_text.bytes));                \
            }                                                                 \
        case AMQP_CHANNEL_CLOSE_METHOD:                                       \
            {                                                                 \
                 amqp_channel_close_t *m =                                    \
                     static_cast<amqp_channel_close_t*>(status.reply.decoded);\
                 throw AMQPException(__FILE__, __LINE__,                      \
                     "%s: server channel error %d, message: %.*s",            \
                     context,                                                 \
                     m->reply_code,                                           \
                     static_cast<int>(m->reply_text.len),                     \
                     static_cast<char*>(m->reply_text.bytes));                \
            }                                                                 \
        default:                                                              \
            throw AMQPException(__FILE__, __LINE__,                           \
                "%s: unknown server error, method id 0x%08X",                 \
                context, status.reply.id);                                    \
        }                                                                     \
    }                                                                         \
} while(0)

class AMQPMessage {
public:

    AMQPMessage(AMQPQueue *queue);
    ~AMQPMessage();

    void setMessage(const char *data, size_t size);
    const std::string& getMessage() const;

    void addHeader(const std::string &name, amqp_bytes_t *value);
    void addHeader(const std::string &name, uint64_t *value);
    void addHeader(const std::string &name, uint8_t *value);

    std::string getHeader(const std::string &name);

    bool hasHeader(const std::string &name) const;

    void setConsumerTag(amqp_bytes_t consumer_tag);
    void setConsumerTag(const std::string &consumer_tag);

    std::string getConsumerTag();

    void setMessageCount(int count);
    int getMessageCount();

    void setExchange(amqp_bytes_t exchange);
    void setExchange(const std::string &exchange);

    std::string getExchange();

    void setRoutingKey(amqp_bytes_t routing_key);
    void setRoutingKey(const std::string &routing_key);

    std::string getRoutingKey();

    uint32_t getDeliveryTag();
    void setDeliveryTag(uint32_t delivery_tag);

    AMQPQueue* getQueue();

private:

    typedef std::map<std::string, std::string> headers_map;

    std::string data;
    std::string exchange;
    std::string routing_key;
    std::string consumer_tag;

    uint32_t    delivery_tag;
    int         message_count;
    AMQPQueue  *queue;
    headers_map headers;
};

class AMQPBase {
public:

    AMQPBase():
        parms(0),
        channelNum(0),
        pmessage(NULL),
        opened(0)
    {
    }
    
    virtual ~AMQPBase();

    int getChannelNum();
    void setParam(short param);
    std::string getName();
    void closeChannel();
    void reopen();

    void setName(const char *name);
    void setName(const std::string &name);

protected:

    amqp_connection_state_t *cnn;

    std::string  name;
    short        parms;
    int          channelNum;
    AMQPMessage *pmessage;
    short        opened;

    void checkClosed(amqp_rpc_reply_t *res);
    void openChannel();
};

typedef int (*AMQPEventFunc)(AMQPMessage*, void*);

class AMQPQueue: public AMQPBase  {
public:

    AMQPQueue(amqp_connection_state_t *cnn, int channelNum);
    AMQPQueue(amqp_connection_state_t *cnn, int channelNum, std::string name);

    virtual ~AMQPQueue();

    void Declare();
    void Declare(const std::string &name);
    void Declare(const std::string &name, short parms);

    void Delete();
    void Delete(const std::string &name);

    void Purge();
    void Purge(const std::string &name);

    void Bind(const std::string &exchangeName, const std::string &key);

    void unBind(const std::string &exchangeName, const std::string &key);

    void Get();
    void Get(short param);

    void Consume();
    void Consume(short param);

    void Cancel(amqp_bytes_t consumer_tag);
    void Cancel(const std::string &consumer_tag);

    void Ack();
    void Ack(uint32_t delivery_tag);

    AMQPMessage* getMessage() {
        return pmessage;
    }

    uint32_t getCount() {
        return count;
    }

    void setConsumerTag(const std::string &consumer_tag);
    amqp_bytes_t getConsumerTag();

    void addEvent(AMQPEvents_e eventType, AMQPEventFunc func, void *ctx);

protected:

    struct AMQPCallback {
        
        AMQPEventFunc  func;
        void          *ctx;

        AMQPCallback(AMQPEventFunc _func, void *_ctx):
            func(_func),
            ctx(_ctx)
        {
        }

        AMQPCallback():
            func(NULL),
            ctx(NULL)
        {
        }
    };

    typedef std::map<AMQPEvents_e, AMQPCallback> events_map;

    events_map   events;
    amqp_bytes_t consumer_tag;
    
    uint32_t     delivery_tag;
    uint32_t     count;

private:

    void sendDeclareCommand();
    void sendDeleteCommand();
    void sendPurgeCommand();
    void sendBindCommand(const char *exchange, const char *key);
    void sendUnBindCommand(const char *exchange, const char *key);
    void sendGetCommand();
    void sendConsumeCommand();
    void sendCancelCommand();
    void sendAckCommand();
    void setHeaders(amqp_basic_properties_t *p);
};


class AMQPExchange: public AMQPBase {
public:

    AMQPExchange(amqp_connection_state_t *cnn, int channelNum);
    AMQPExchange(amqp_connection_state_t *cnn, int channelNum,
        const std::string &name);

    virtual ~AMQPExchange() {};

    void Declare();
    void Declare(const std::string &name);
    void Declare(const std::string &name, const std::string &type);
    void Declare(const std::string &name, const std::string &type,
        short parms);

    void Delete();
    void Delete(const std::string &name);

    void Bind(const std::string &queueName);
    void Bind(const std::string &queueName, const std::string &key);

    void Publish(const std::string &message, const std::string &key);

    void setHeader(const std::string &name, int value);
    void setHeader(const std::string &name, const std::string &value);

private:

    std::string type;

    typedef std::map<std::string, std::string> sHeaders_map;
    typedef std::map<std::string, int>         iHeaders_map;

    sHeaders_map sHeaders;
    iHeaders_map iHeaders;

private:

    AMQPExchange();

    void checkType();
    void sendDeclareCommand();
    void sendDeleteCommand();
    void sendPublishCommand();

    void sendBindCommand(const char *queueName, const char *key);
    void sendPublishCommand(const char *message, const char *key);
    void sendCommand();
    void checkClosed(amqp_rpc_reply_t *res);
};

class AMQP {
public:

    AMQP();
    AMQP(const std::string &cnnStr);
    AMQP(const std::string &user, const std::string &password, const std::string &host,
        int port, const std::string &vhost);
    
    ~AMQP();

    AMQPExchange* createExchange();
    AMQPExchange* createExchange(const std::string &name);

    AMQPQueue* createQueue();
    AMQPQueue* createQueue(const std::string &name);

    void printConnect();
    void closeChannel();

private:

    int port;

    std::string host;
    std::string vhost;
    std::string user;
    std::string password;

    int sockfd;
    int channelNumber;

    AMQPExchange *exchange;

    amqp_connection_state_t  cnn;
    
    std::vector<AMQPBase*> channels;

private:

    AMQP(AMQP &ob);

    void init();
    void initDefault();
    void connect();
    void parseCnnString(const std::string &cnnString);
    void parseHostPort(const std::string &hostPortString);
    void parseUserStr(const std::string &userString);
    void sockConnect();
    void login();
};

} // namespace

#endif //__AMQPCPP
