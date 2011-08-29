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


    AMQPException(const char *file, int line, amqp_rpc_reply_t *res);
    AMQPException(const char *file, int line, const char *fmt, ...);

    virtual ~AMQPException() throw() {
        free(msg);
    };

    virtual const char *what() const throw() {
        return msg == NULL ? "" : msg;
    }

private:

    char  buff[1024];
    char *msg;
    char  line_str[sizeof("2147483647")];

    const char* sanitize_file_name(const char *file);
    void append_position(const char *file, int line);
};

#define THROW_AMQP_EXC(args...)  (throw AMQPException(__FILE__, __LINE__, args))

#define THROW_AMQP_EXC_IF_FAILED(status, args...) do {  \
    if (!(status)) {                                    \
        throw AMQPException(__FILE__, __LINE__, args);  \
    }                                                   \
} while(0)

class AMQPMessage {
public:

    AMQPMessage(AMQPQueue *queue);
    ~AMQPMessage();

    void setMessage(const char *data, size_t size);
    const char* getMessage();

    void addHeader(std::string name, amqp_bytes_t *value);
    void addHeader(std::string name, uint64_t *value);
    void addHeader(std::string name, uint8_t *value);
    std::string getHeader(std::string name);

    void setConsumerTag(amqp_bytes_t consumer_tag);
    void setConsumerTag(std::string consumer_tag);
    std::string getConsumerTag();

    void setMessageCount(int count);
    int getMessageCount();

    void setExchange(amqp_bytes_t exchange);
    void setExchange(std::string exchange);
    std::string getExchange();

    void setRoutingKey(amqp_bytes_t routing_key);
    void setRoutingKey(std::string routing_key);
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

    ~AMQPBase();
    int getChannelNum();
    void setParam(short param);
    std::string getName();
    void closeChannel();
    void reopen();
    void setName(const char * name);
    void setName(std::string name);

protected:

    amqp_connection_state_t *cnn;

    std::string  name;
    short        parms;
    int          channelNum;
    AMQPMessage *pmessage;
    short        opened;

    void checkReply(amqp_rpc_reply_t *res);
    void checkClosed(amqp_rpc_reply_t *res);
    void openChannel();
};

class AMQPQueue: public AMQPBase  {
public:

    AMQPQueue(amqp_connection_state_t *cnn, int channelNum);
    AMQPQueue(amqp_connection_state_t *cnn, int channelNum, std::string name);

    void Declare();
    void Declare(std::string name);
    void Declare(std::string name, short parms);

    void Delete();
    void Delete(std::string name);

    void Purge();
    void Purge(std::string name);

    void Bind(std::string exchangeName, std::string key);

    void unBind(std::string exchangeName, std::string key);

    void Get();
    void Get(short param);

    void Consume();
    void Consume(short param);

    void Cancel(amqp_bytes_t consumer_tag);
    void Cancel(std::string consumer_tag);

    void Ack();
    void Ack(uint32_t delivery_tag);

    AMQPMessage* getMessage() {
        return pmessage;
    }

    uint32_t getCount() {
        return count;
    }

    void setConsumerTag(std::string consumer_tag);
    amqp_bytes_t getConsumerTag();

    void addEvent(AMQPEvents_e eventType, int (*event)(AMQPMessage*));

    ~AMQPQueue();

protected:

    typedef std::map<AMQPEvents_e, int(*)(AMQPMessage *)> events_map;

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
    AMQPExchange(amqp_connection_state_t *cnn, int channelNum, std::string name);

    void Declare();
    void Declare(std::string name);
    void Declare(std::string name, std::string type);
    void Declare(std::string name, std::string type, short parms);

    void Delete();
    void Delete(std::string name);

    void Bind(std::string queueName);
    void Bind(std::string queueName, std::string key);

    void Publish(std::string message, std::string key);

    void setHeader(std::string name, int value);
    void setHeader(std::string name, std::string value);

private:

    std::string type;

    std::map<std::string, std::string> sHeaders;
    std::map<std::string, int>         iHeaders;

private:

    AMQPExchange();

    void checkType();
    void sendDeclareCommand();
    void sendDeleteCommand();
    void sendPublishCommand();

    void sendBindCommand(const char *queueName, const char *key);
    void sendPublishCommand(const char *message, const char *key);
    void sendCommand();
    void checkReply(amqp_rpc_reply_t *res);
    void checkClosed(amqp_rpc_reply_t *res);
};

class AMQP {
public:

    AMQP();
    AMQP(std::string cnnStr);
    ~AMQP();

    AMQPExchange* createExchange();
    AMQPExchange* createExchange(std::string name);

    AMQPQueue* createQueue();
    AMQPQueue* createQueue(std::string name);

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

    amqp_connection_state_t cnn;
    AMQPExchange *exchange;

    std::vector<AMQPBase*> channels;

private:

    AMQP(AMQP &ob);

    void init();
    void initDefault();
    void connect();
    void parseCnnString(std::string cnnString);
    void parseHostPort(std::string hostPortString);
    void parseUserStr(std::string userString);
    void sockConnect();
    void login();
};

} // namespace

#endif //__AMQPCPP
