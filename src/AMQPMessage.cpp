#include "AMQPcpp.h"

namespace amqpcpp {

AMQPMessage::AMQPMessage(AMQPQueue *queue)
{
    this->queue = queue;
    message_count = -1;
}

AMQPMessage::~AMQPMessage()
{
}

void
AMQPMessage::setMessage(const char *ptr, size_t size)
{
    data.clear();

    if (!ptr) {
        return;
    }

    data.assign(ptr, size);
}

const char*
AMQPMessage::getMessage()
{
    return data.c_str();
}

std::string
AMQPMessage::getConsumerTag()
{
    return this->consumer_tag;
}

void
AMQPMessage::setConsumerTag(amqp_bytes_t consumer_tag)
{
    this->consumer_tag.assign((char*)consumer_tag.bytes, consumer_tag.len);
}

void
AMQPMessage::setConsumerTag(std::string consumer_tag)
{
    this->consumer_tag = consumer_tag;
}

void
AMQPMessage::setDeliveryTag(uint32_t delivery_tag)
{
    this->delivery_tag = delivery_tag;
}

uint32_t
AMQPMessage::getDeliveryTag()
{
    return this->delivery_tag;
}

void
AMQPMessage::setMessageCount(int count)
{
    this->message_count = count;
}

int
AMQPMessage::getMessageCount()
{
    return message_count;
}

void
AMQPMessage::setExchange(amqp_bytes_t exchange)
{
    if (exchange.len) {
        this->exchange.assign((char*)exchange.bytes, exchange.len);
    }
}

void
AMQPMessage::setExchange(std::string exchange)
{
    this->exchange = exchange;
}

std::string
AMQPMessage::getExchange()
{
    return exchange;
}

void
AMQPMessage::setRoutingKey(amqp_bytes_t routing_key)
{
    if (routing_key.len) {
        this->routing_key.assign((char*)routing_key.bytes, routing_key.len);
    }
}

void
AMQPMessage::setRoutingKey(std::string routing_key)
{
    this->routing_key = routing_key;
}

std::string
AMQPMessage::getRoutingKey()
{
    return routing_key;
}

void
AMQPMessage::addHeader(std::string name, amqp_bytes_t *value)
{
    std::string svalue;

    svalue.assign((const char *)value->bytes, value->len);
    headers.insert(headers_map::value_type(name, svalue));
}

void
AMQPMessage::addHeader(std::string name, uint64_t *value)
{
    char ivalue[32];

    bzero(ivalue, 32);
    snprintf(ivalue, sizeof(ivalue), "%"PRIu64, *value);
    headers.insert(headers_map::value_type(name, std::string(ivalue)));
}

void
AMQPMessage::addHeader(std::string name, uint8_t *value)
{
    char ivalue[4];

    bzero(ivalue, 4);
    snprintf(ivalue, sizeof(ivalue), "%"PRIu8, *value);
    headers.insert(headers_map::value_type(name, std::string(ivalue)));
}

std::string
AMQPMessage::getHeader(std::string name)
{
    headers_map::iterator header_it = headers.find(name);

    if (header_it == headers.end()) {
        return std::string("");
    } else {
        return header_it->second;
    }
}

AMQPQueue*
AMQPMessage::getQueue()
{
    return queue;
}

} // namespace
