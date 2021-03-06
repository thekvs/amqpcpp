#include "AMQPcpp.h"

namespace amqpcpp {

AMQPExchange::AMQPExchange(amqp_connection_state_t *cnn, int channelNum): AMQPBase()
{
    this->cnn = cnn;
    this->channelNum = channelNum;

    openChannel();
}

AMQPExchange::AMQPExchange(amqp_connection_state_t *cnn, int channelNum,
    const std::string &name): AMQPBase()
{
    this->cnn = cnn;
    this->channelNum = channelNum;
    this->name = name;

    openChannel();
}

void
AMQPExchange::Declare()
{
    this->parms = 0;

    sendDeclareCommand();
}

void
AMQPExchange::Declare(const std::string &name)
{
    this->parms = 0;
    this->name = name;

    sendDeclareCommand();
}

void
AMQPExchange::Declare(const std::string &name, const std::string &type)
{
    this->parms = 0;
    this->name = name;
    this->type = type;

    sendDeclareCommand();
}

void
AMQPExchange::Declare(const std::string &name, const std::string &type, short parms)
{
    this->name = name;
    this->type = type;
    this->parms = parms;

    sendDeclareCommand();
}

void
AMQPExchange::sendDeclareCommand()
{
    checkType();

    amqp_bytes_t exchange = amqp_cstring_bytes(name.c_str());
    amqp_bytes_t exchangetype = amqp_cstring_bytes(type.c_str());

    amqp_table_t args;
    memset(&args, 0, sizeof(args));

    amqp_boolean_t passive = (parms & AMQP_PASSIVE)		? 1:0;
    //amqp_boolean_t autodelete = (parms & AMQP_AUTODELETE)	? 1:0;
    amqp_boolean_t durable = (parms & AMQP_DURABLE)		? 1:0;

    //amqp_exchange_declare(*cnn, (amqp_channel_t) 1, exchange, exchangetype, passive, durable, autodelete, args ); //for some reason rabbitmq-c doesn't have auto-delete now...
    amqp_exchange_declare(*cnn, (amqp_channel_t)1, exchange, exchangetype, passive, durable, args);

    amqp_rpc_reply_t res = amqp_get_rpc_reply(*cnn);
    THROW_AMQP_EXC_IF_FAILED(res, "exchange declare command");
}

void
AMQPExchange::checkType()
{
    short isErr = 1;

    if (type == "direct") {
        isErr = 0;
    }

    if (type == "fanout") {
        isErr = 0;
    }

    if (type == "topic") {
        isErr = 0;
    }

    if (isErr) {
        THROW_AMQP_EXC("type of the exchange must be direct|fanout|topic");
    }
}

void
AMQPExchange::Delete()
{
    if (!name.size()) {
        THROW_AMQP_EXC("name of the exchange not set");
    }

    sendDeleteCommand();
}

void
AMQPExchange::Delete(const std::string &name)
{
    this->name = name;

    sendDeleteCommand();
}

void
AMQPExchange::sendDeleteCommand()
{
    amqp_bytes_t exchange = amqp_cstring_bytes(name.c_str());

    amqp_exchange_delete_t s;

    memset(&s, 0, sizeof(s));
    s.ticket = 0;
    s.exchange = exchange;
    s.if_unused = (AMQP_IFUNUSED & parms) ? 1:0;
    s.nowait = (AMQP_NOWAIT & parms) ? 1:0;

    amqp_method_number_t method_ok = AMQP_EXCHANGE_DELETE_OK_METHOD;

    amqp_rpc_reply_t res = amqp_simple_rpc(
        *cnn,
        channelNum,
        AMQP_EXCHANGE_DELETE_METHOD,
        &method_ok,
        &s
        );

    THROW_AMQP_EXC_IF_FAILED(res, "exchange delete command");
}

void
AMQPExchange::Bind(const std::string &name)
{
    if (type != "fanout") {
        THROW_AMQP_EXC("key is NULL, it can only be used for the fanout type");
    }

    sendBindCommand(name.c_str(), NULL);
}

void
AMQPExchange::Bind(const std::string &name, const std::string &key)
{
    sendBindCommand(name.c_str(), key.c_str());
}

void
AMQPExchange::sendBindCommand(const char *queue, const char *key)
{

    amqp_bytes_t queueByte = amqp_cstring_bytes(queue);
    amqp_bytes_t exchangeByte = amqp_cstring_bytes(name.c_str());
    amqp_bytes_t keyByte = amqp_cstring_bytes(key);

    amqp_queue_bind_t s;

    memset(&s, 0, sizeof(s));
    s.ticket = 0;
    s.queue = queueByte;
    s.exchange = exchangeByte;
    s.routing_key = keyByte;
    s.nowait = (AMQP_NOWAIT & parms) ? 1:0;

    s.arguments.num_entries = 0;
    s.arguments.entries = NULL;

    amqp_method_number_t method_ok = AMQP_QUEUE_BIND_OK_METHOD;
    amqp_rpc_reply_t res = amqp_simple_rpc(
        *cnn,
        channelNum,
        AMQP_QUEUE_BIND_METHOD,
        &method_ok,
        &s
        );

    THROW_AMQP_EXC_IF_FAILED(res, "exchange bind command");
}

void
AMQPExchange::Publish(const std::string &message, const std::string &key)
{
    sendPublishCommand(message.c_str(), key.c_str());
}

void
AMQPExchange::sendPublishCommand(const char *message, const char *key)
{
    amqp_bytes_t exchangeByte = amqp_cstring_bytes(name.c_str());
    amqp_bytes_t keyrouteByte = amqp_cstring_bytes(key);
    amqp_bytes_t messageByte = amqp_cstring_bytes(message);

    amqp_basic_properties_t props;

    memset(&props, 0, sizeof(props));

    if (sHeaders.find("Content-type") != sHeaders.end()) {
        props.content_type = amqp_cstring_bytes(sHeaders["Content-type"].c_str());
    } else {
        props.content_type = amqp_cstring_bytes("text/plain");
    }

    if (iHeaders.find("Delivery-mode") != iHeaders.end()) {
        props.delivery_mode = (uint8_t)iHeaders["Delivery-mode"];
    } else {
        props.delivery_mode = 2; // persistent delivery mode
    }

    props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG;

    if (sHeaders.find("Content-encoding") != sHeaders.end()) {
        props.content_encoding = amqp_cstring_bytes(sHeaders["Content-encoding"].c_str());
        props._flags += AMQP_BASIC_CONTENT_ENCODING_FLAG;
    }

    if (sHeaders.find("message_id") != sHeaders.end()) {
        props.message_id = amqp_cstring_bytes(sHeaders["message_id"].c_str());
        props._flags += AMQP_BASIC_MESSAGE_ID_FLAG;
    }

    if (sHeaders.find("user_id") != sHeaders.end()) {
        props.user_id = amqp_cstring_bytes(sHeaders["user_id"].c_str());
        props._flags += AMQP_BASIC_USER_ID_FLAG;
    }

    if (sHeaders.find("app_id") != sHeaders.end()) {
        props.app_id = amqp_cstring_bytes(sHeaders["app_id"].c_str());
        props._flags += AMQP_BASIC_APP_ID_FLAG;
    }

    if (sHeaders.find("cluster_id") != sHeaders.end()) {
        props.cluster_id = amqp_cstring_bytes(sHeaders["cluster_id"].c_str());
        props._flags += AMQP_BASIC_CLUSTER_ID_FLAG;
    }

    if (sHeaders.find("correlation_id") != sHeaders.end()) {
        props.correlation_id = amqp_cstring_bytes(sHeaders["correlation_id"].c_str());
        props._flags += AMQP_BASIC_CORRELATION_ID_FLAG;
    }

    if (iHeaders.find("priority") != iHeaders.end()) {
        props.priority = (uint8_t)iHeaders["priority"];
        props._flags += AMQP_BASIC_PRIORITY_FLAG;
    }

    if (iHeaders.find("timestamp") != iHeaders.end()) {
        props.timestamp = (uint64_t)iHeaders["timestamp"];
        props._flags += AMQP_BASIC_TIMESTAMP_FLAG;
    }

    if (sHeaders.find("Expiration") != sHeaders.end()) {
        props.expiration = amqp_cstring_bytes(sHeaders["Expiration"].c_str());
        props._flags += AMQP_BASIC_EXPIRATION_FLAG;
    }

    if (sHeaders.find("type") != sHeaders.end()) {
        props.type = amqp_cstring_bytes(sHeaders["type"].c_str());
        props._flags += AMQP_BASIC_TYPE_FLAG;
    }

    if (sHeaders.find("Reply-to") != sHeaders.end()) {
        props.reply_to = amqp_cstring_bytes(sHeaders["Reply-to"].c_str());
        props._flags += AMQP_BASIC_REPLY_TO_FLAG;
    }

    short mandatory = (parms & AMQP_MANDATORY) ? 1:0;
    short immediate = (parms & AMQP_IMMIDIATE) ? 1:0;

    amqp_basic_publish(
        *cnn,
        1,
        exchangeByte,
        keyrouteByte,
        mandatory,
        immediate,
        &props,
        messageByte
        );
}

void
AMQPExchange::setHeader(const std::string &name, int value)
{
    iHeaders.insert(iHeaders_map::value_type(name, value));
}

void
AMQPExchange::setHeader(const std::string &name, const std::string &value)
{
    sHeaders.insert(sHeaders_map::value_type(name, value));
}

} // namespace

