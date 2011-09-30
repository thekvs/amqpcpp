#include "AMQPcpp.h"

namespace amqpcpp {

AMQPQueue::AMQPQueue(amqp_connection_state_t *cnn, int channelNum): AMQPBase()
{
    this->cnn = cnn;
    this->channelNum = channelNum;

    consumer_tag.bytes = NULL;
    consumer_tag.len = 0;
    delivery_tag = 0;

    openChannel();
}

AMQPQueue::AMQPQueue(amqp_connection_state_t *cnn, int channelNum, std::string name): AMQPBase()
{
    this->cnn = cnn;
    this->channelNum = channelNum;
    this->name = name;

    consumer_tag.bytes = NULL;
    consumer_tag.len = 0;
    delivery_tag = 0;

    openChannel();
}

AMQPQueue::~AMQPQueue()
{
    this->closeChannel();

    if (pmessage) {
        delete pmessage;
    }
}

// Declare command /* 50, 10; 3276810 */
void
AMQPQueue::Declare()
{
    parms = 0;
    sendDeclareCommand();
}

void
AMQPQueue::Declare(const std::string &name)
{
    this->parms = AMQP_AUTODELETE;
    this->name = name;

    sendDeclareCommand();
}

void
AMQPQueue::Declare(const std::string &name, short parms)
{
    this->parms = parms;
    this->name = name;

    sendDeclareCommand();
}

void
AMQPQueue::sendDeclareCommand()
{
    if (!name.size()) {
        THROW_AMQP_EXC("queue must to have the name");
    }

    amqp_bytes_t queue_name = amqp_cstring_bytes(name.c_str());

    /*
       amqp_basic_properties_t props;
       props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG;
       props.content_type = amqp_cstring_bytes("text/plain");
     */
    amqp_table_t args;

    memset(&args, 0, sizeof(args));

    amqp_boolean_t exclusive  = (parms & AMQP_EXCLUSIVE)	? 1:0;
    amqp_boolean_t passive    = (parms & AMQP_PASSIVE)		? 1:0;
    amqp_boolean_t autodelete = (parms & AMQP_AUTODELETE)	? 1:0;
    amqp_boolean_t durable    = (parms & AMQP_DURABLE)		? 1:0;

    amqp_queue_declare_t s;

    memset(&s, 0, sizeof(s));

    s.ticket = 0;
    s.queue = queue_name;
    s.passive = passive;
    s.durable = durable;
    s.exclusive = exclusive;
    s.auto_delete = autodelete;
    s.nowait = 0;
    s.arguments = args;

    amqp_method_number_t method_ok = AMQP_QUEUE_DECLARE_OK_METHOD;
    amqp_rpc_reply_t res = amqp_simple_rpc(*cnn, channelNum, AMQP_QUEUE_DECLARE_METHOD, &method_ok, &s);
    THROW_AMQP_EXC_IF_FAILED(res, "declare queue");

    amqp_release_buffers(*cnn);

    if (res.reply_type == AMQP_RESPONSE_NONE) {
        THROW_AMQP_EXC("error in the QUEUE.DECLARE command, response is none");
    }

    if (res.reply.id == AMQP_CHANNEL_CLOSE_METHOD) {
        amqp_channel_close_t *err = (amqp_channel_close_t *)res.reply.decoded;

        int c_id = (int)err->class_id;
        opened = 0;

        THROW_AMQP_EXC("server error %u, message '%s' class=%d method=%u",
            err->reply_code, (char*)err->reply_text.bytes,
            c_id, err->method_id);
    } else if (res.reply.id == AMQP_QUEUE_DECLARE_OK_METHOD) {
        amqp_queue_declare_ok_t* data = (amqp_queue_declare_ok_t*) res.reply.decoded;
        count = data->message_count;
    } else {
        THROW_AMQP_EXC("error in the Declare command receive method=%d",
            res.reply.id);
    }
}

// Delete command  /* 50, 40; 3276840 */
void
AMQPQueue::Delete()
{
    if (!name.size()) {
        THROW_AMQP_EXC("name of the queue is not set");
    }

    sendDeleteCommand();
}

void
AMQPQueue::Delete(const std::string &name)
{
    this->name = name;
    sendDeleteCommand();
}

void
AMQPQueue::sendDeleteCommand()
{
    amqp_bytes_t queue = amqp_cstring_bytes(name.c_str());

    amqp_queue_delete_t s;

    memset(&s, 0, sizeof(s));
    s.ticket = 0;
    s.queue = queue;
    s.if_unused = (AMQP_IFUNUSED & parms) ? 1:0;
    s.nowait = (AMQP_NOWAIT & parms) ? 1:0;

    amqp_method_number_t method_ok = AMQP_QUEUE_DELETE_OK_METHOD;

    amqp_rpc_reply_t res = amqp_simple_rpc(*cnn, channelNum, AMQP_QUEUE_DELETE_METHOD, &method_ok, &s);
    THROW_AMQP_EXC_IF_FAILED(res, "delete queue");
}

// Purge command /* 50, 30; 3276830 */
void
AMQPQueue::Purge()
{
    if (!name.size()) {
        THROW_AMQP_EXC("name of the queue is not set");
    }

    sendPurgeCommand();
}

void
AMQPQueue::Purge(const std::string &name)
{
    this->name = name;

    sendPurgeCommand();
}

void
AMQPQueue::sendPurgeCommand()
{
    amqp_bytes_t queue = amqp_cstring_bytes(name.c_str());

    amqp_queue_delete_t s;

    memset(&s, 0, sizeof(s));
    s.ticket = 0;
    s.queue = queue;
    s.nowait = (AMQP_NOWAIT & parms) ? 1:0;

    amqp_method_number_t method_ok = AMQP_QUEUE_PURGE_OK_METHOD;

    amqp_rpc_reply_t res = amqp_simple_rpc(*cnn, channelNum, AMQP_QUEUE_PURGE_METHOD, &method_ok , &s);
    THROW_AMQP_EXC_IF_FAILED(res, "purge queue");
}

// Bind command /* 50, 20; 3276820 */
void
AMQPQueue::Bind(const std::string &name, const std::string &key)
{
    sendBindCommand(name.c_str(), key.c_str());
}

void
AMQPQueue::sendBindCommand(const char *exchange, const char *key)
{
    amqp_bytes_t queueByte = amqp_cstring_bytes(name.c_str());
    amqp_bytes_t exchangeByte = amqp_cstring_bytes(exchange);
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
    amqp_rpc_reply_t res = amqp_simple_rpc(*cnn, channelNum, AMQP_QUEUE_BIND_METHOD, &method_ok, &s);
    THROW_AMQP_EXC_IF_FAILED(res, "bind queue");
}


// UnBind command /* 50, 50; 3276850 */
void
AMQPQueue::unBind(const std::string &name, const std::string &key)
{
    sendUnBindCommand(name.c_str(), key.c_str());
}

void
AMQPQueue::sendUnBindCommand(const char *exchange, const char *key)
{
    amqp_bytes_t queueByte = amqp_cstring_bytes(name.c_str());
    amqp_bytes_t exchangeByte = amqp_cstring_bytes(exchange);
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

    amqp_method_number_t method_ok = AMQP_QUEUE_UNBIND_OK_METHOD;
    amqp_rpc_reply_t res = amqp_simple_rpc(*cnn, channelNum, AMQP_QUEUE_UNBIND_METHOD, &method_ok, &s);
    THROW_AMQP_EXC_IF_FAILED(res, "unbind queue");
}


// GET method /* 60, 71; 3932231 */
void
AMQPQueue::Get()
{
    parms = 0;
    sendGetCommand();
}

void
AMQPQueue::Get(short parms)
{
    this->parms = parms;
    sendGetCommand();
}

void
AMQPQueue::sendGetCommand()
{
    amqp_bytes_t queueByte = amqp_cstring_bytes(name.c_str());

    amqp_basic_get_t s;

    memset(&s, 0, sizeof(s));
    s.ticket = 0;
    s.queue = queueByte;
    s.no_ack = (AMQP_NOACK & parms) ? 1:0;

    amqp_method_number_t replies[] = {
        AMQP_BASIC_GET_OK_METHOD,
        AMQP_BASIC_GET_EMPTY_METHOD,
        AMQP_CONNECTION_CLOSE_METHOD,
        0
    };

    amqp_rpc_reply_t res = amqp_simple_rpc(*cnn, channelNum, AMQP_BASIC_GET_METHOD, replies, &s);

    // 3932232 GET_EMPTY
    // 3932231 GET_OK
    // 1310760 CHANNEL_CLOSE

    amqp_frame_t frame;

    amqp_release_buffers(*cnn);

    if (pmessage) {
        delete(pmessage);
    }

    pmessage = new AMQPMessage(this);

    if (res.reply_type == AMQP_RESPONSE_NONE) {
        THROW_AMQP_EXC("error in the Get command, response is none");
    }

    if (res.reply.id == AMQP_CHANNEL_CLOSE_METHOD) {
        amqp_channel_close_t *err = (amqp_channel_close_t *) res.reply.decoded;

        opened = 0;
        THROW_AMQP_EXC("server error %u, message '%s' class=%d method=%u ",
            err->reply_code, (char*)err->reply_text.bytes,
            (int)err->class_id, err->method_id);

    } else if (res.reply.id == AMQP_BASIC_GET_EMPTY_METHOD) {
        pmessage->setMessageCount(-1);
        return;
    } else if (res.reply.id == AMQP_BASIC_GET_OK_METHOD) {
        amqp_basic_get_ok_t* data = (amqp_basic_get_ok_t*) res.reply.decoded;

        delivery_tag = data->delivery_tag;
        pmessage->setDeliveryTag(data->delivery_tag);

        std::string s;

        amqp_bytes_t exName = data->exchange;

        s.assign(static_cast<const char*>(exName.bytes), exName.len);
        pmessage->setExchange(s);

        amqp_bytes_t routingKey = data->routing_key;

        s.assign(static_cast<const char*>(routingKey.bytes), routingKey.len);
        pmessage->setRoutingKey(s);

        pmessage->setMessageCount(data->message_count);
    } else {
        THROW_AMQP_EXC("error in the Get command  receive method=%d", res.reply.id);
    }

    int               result;
    std::vector<char> tmp;
    size_t            frame_len;
    char             *start, *end;

    while (1) { //receive frames...
        amqp_maybe_release_buffers(*cnn);
        result = amqp_simple_wait_frame(*cnn, &frame);

        if (result < 0) {
            THROW_AMQP_EXC("read frame error");
        }

        if (frame.frame_type == AMQP_FRAME_HEADER) {
            amqp_basic_properties_t *p = (amqp_basic_properties_t *)frame.payload.properties.decoded;
            this->setHeaders(p);
            continue;
        }

        if (frame.frame_type == AMQP_FRAME_BODY) {
            frame_len = frame.payload.body_fragment.len;
            start = static_cast<char*>(frame.payload.body_fragment.bytes);
            end = static_cast<char*>(frame.payload.body_fragment.bytes) + frame_len;

            tmp.insert(tmp.end(), start, end);

            if (frame_len < FRAME_MAX - HEADER_FOOTER_SIZE) {
                break;
            }

            continue;
        }
    }

    if (tmp.size() > 0) {
        pmessage->setMessage(&tmp[0], tmp.size());
    }

    amqp_release_buffers(*cnn);
}

void
AMQPQueue::addEvent(AMQPEvents_e eventType, AMQPEventFunc func, void *ctx)
{
    if (events.find(eventType) != events.end()) {
        THROW_AMQP_EXC("event allready exists");
    }

    events[eventType] = AMQPCallback(func, ctx);
}

void
AMQPQueue::Consume()
{
    parms = 0;

    sendConsumeCommand();
}

void
AMQPQueue::Consume(short parms)
{
    this->parms = parms;

    sendConsumeCommand();
}

void
AMQPQueue::setConsumerTag(const std::string &consumer_tag)
{
    this->consumer_tag = amqp_cstring_bytes(consumer_tag.c_str());
}

void
AMQPQueue::sendConsumeCommand()
{
    amqp_bytes_t queueByte = amqp_cstring_bytes(name.c_str());

    /*
       amqp_basic_consume_ok_t * res = amqp_basic_consume( *cnn, channelNum,
       queueByte, consumer_tag,
       0, //amqp_boolean_t no_local,
       1, // amqp_boolean_t no_ack,
       0  //amqp_boolean_t exclusive
       ); 
     */

    amqp_method_number_t replies[] = {
        AMQP_BASIC_CONSUME_OK_METHOD,
        AMQP_CONNECTION_CLOSE_METHOD,
        AMQP_BASIC_CANCEL_OK_METHOD,
        0
    };

    amqp_basic_consume_t s;

    memset(&s, 0, sizeof(s));
    s.ticket = channelNum;
    s.queue = queueByte;
    s.consumer_tag = consumer_tag;
    s.no_local = (AMQP_NOLOCAL & parms)     ? 1:0;
    s.no_ack = (AMQP_NOACK & parms)         ? 1:0;
    s.exclusive = (AMQP_EXCLUSIVE & parms)  ? 1:0;

    amqp_rpc_reply_t res = amqp_simple_rpc(*cnn, channelNum, AMQP_BASIC_CONSUME_METHOD, replies, &s);
    THROW_AMQP_EXC_IF_FAILED(res, "consume method");

    if (res.reply.id == AMQP_BASIC_CANCEL_OK_METHOD) {
        return;
    }

    std::auto_ptr<AMQPMessage> message(new AMQPMessage(this));
    pmessage = message.get();

    amqp_frame_t frame;

    size_t body_received;
    size_t body_target;

    while(1) {
        amqp_maybe_release_buffers(*cnn);

        int result = amqp_simple_wait_frame(*cnn, &frame);

        if (result < 0) {
            return;
        }

        if (frame.frame_type != AMQP_FRAME_METHOD){
            continue;
        }

        if (frame.payload.method.id == AMQP_BASIC_CANCEL_OK_METHOD) {
            events_map::iterator event_it = events.find(AMQP_CANCEL);
            if (event_it != events.end()) {
                AMQPEventFunc func = event_it->second.func;
                void *ctx = event_it->second.ctx;
                (*func)(pmessage, ctx);
            }
            break;
        }

        if (frame.payload.method.id != AMQP_BASIC_DELIVER_METHOD) {
            continue;
        }

        amqp_basic_deliver_t *delivery = (amqp_basic_deliver_t*)frame.payload.method.decoded;

        delivery_tag = delivery->delivery_tag;
        pmessage->setConsumerTag(delivery->consumer_tag);
        pmessage->setDeliveryTag(delivery->delivery_tag);

        pmessage->setExchange(delivery->exchange);
        pmessage->setRoutingKey(delivery->routing_key);

        result = amqp_simple_wait_frame(*cnn, &frame);

        if (result < 0) {
            THROW_AMQP_EXC("invalid frame");
        }

        if (frame.frame_type != AMQP_FRAME_HEADER) {
            THROW_AMQP_EXC("invalid frame type: %i", frame.frame_type);
        }

        amqp_basic_properties_t *p = (amqp_basic_properties_t*)(frame.payload.properties.decoded);

        this->setHeaders(p);

        body_target = frame.payload.properties.body_size;
        body_received = 0;

        std::vector<char> tmp;
        size_t            frame_len;
        char             *start, *end;

        tmp.reserve(body_target);

        while (body_received < body_target) {
            result = amqp_simple_wait_frame(*cnn, &frame);
            
            if (result < 0) {
                break;
            }

            if (frame.frame_type != AMQP_FRAME_BODY) {
                THROW_AMQP_EXC("returned frame has no body");
            }

            frame_len = frame.payload.body_fragment.len;

            start = static_cast<char*>(frame.payload.body_fragment.bytes);
            end = static_cast<char*>(frame.payload.body_fragment.bytes) + frame_len;

            tmp.insert(tmp.end(), start, end);

            body_received += frame_len;
        }

        pmessage->setMessage(&tmp[0], tmp.size());

        events_map::iterator event_it = events.find(AMQP_MESSAGE);

        if (event_it != events.end()) {
            AMQPEventFunc func = event_it->second.func;
            void *ctx = event_it->second.ctx;

            int rc = (*func)(pmessage, ctx);

            if (rc != 0) {
                break;
            }
        }
    }
}

void
AMQPQueue::setHeaders(amqp_basic_properties_t *p)
{
    if (pmessage == NULL) {
        return;
    }

    if (p->_flags & AMQP_BASIC_CONTENT_TYPE_FLAG) {
        pmessage->addHeader("Content-type", &p->content_type);
    }

    if (p->_flags & AMQP_BASIC_CONTENT_ENCODING_FLAG) {
        pmessage->addHeader("Content-encoding", &p->content_encoding);
    }


    if (p->_flags & AMQP_BASIC_DELIVERY_MODE_FLAG) {
        pmessage->addHeader("Delivery-mode", &p->delivery_mode);
    }

    if (p->_flags & AMQP_BASIC_MESSAGE_ID_FLAG) {
        pmessage->addHeader("message_id", &p->message_id);
    }

    if (p->_flags & AMQP_BASIC_USER_ID_FLAG) {
        pmessage->addHeader("user_id", &p->user_id);
    }

    if (p->_flags & AMQP_BASIC_APP_ID_FLAG) {
        pmessage->addHeader("app_id", &p->app_id);
    }

    if (p->_flags & AMQP_BASIC_CLUSTER_ID_FLAG) {
        pmessage->addHeader("cluster_id", &p->cluster_id);
    }

    if (p->_flags & AMQP_BASIC_CORRELATION_ID_FLAG) {
        pmessage->addHeader("correlation_id", &p->correlation_id);
    }

    if (p->_flags & AMQP_BASIC_PRIORITY_FLAG) {
        pmessage->addHeader("priority", &p->priority);
    }

    if (p->_flags & AMQP_BASIC_TIMESTAMP_FLAG) {
        pmessage->addHeader("timestamp", &p->timestamp);
    }

    if (p->_flags & AMQP_BASIC_EXPIRATION_FLAG) {
        pmessage->addHeader("Expiration", &p->expiration);
    }

    if (p->_flags & AMQP_BASIC_TYPE_FLAG) {
        pmessage->addHeader("type", &p->type);
    }

    if (p->_flags & AMQP_BASIC_REPLY_TO_FLAG) {
        pmessage->addHeader("Reply-to", &p->reply_to);
    }
}

void
AMQPQueue::Cancel(const std::string &consumer_tag)
{
    this->consumer_tag = amqp_cstring_bytes(consumer_tag.c_str());

    sendCancelCommand();
}

void
AMQPQueue::Cancel(amqp_bytes_t consumer_tag)
{
    this->consumer_tag.len = consumer_tag.len;
    this->consumer_tag.bytes = consumer_tag.bytes;

    sendCancelCommand();
}

void
AMQPQueue::sendCancelCommand()
{
    amqp_basic_cancel_t s;

    memset(&s, 0, sizeof(s));
    s.consumer_tag = consumer_tag;
    s.nowait = (AMQP_NOWAIT & parms) ? 1:0;

    amqp_send_method(*cnn, channelNum, AMQP_BASIC_CANCEL_METHOD, &s);
}

amqp_bytes_t
AMQPQueue::getConsumerTag()
{
    return consumer_tag;
}

void
AMQPQueue::Ack()
{
    if (!delivery_tag) {
        THROW_AMQP_EXC("delivery tag is not set");
    }

    sendAckCommand();
}

void
AMQPQueue::Ack(uint32_t delivery_tag)
{
    this->delivery_tag = delivery_tag;

    sendAckCommand();
}

void
AMQPQueue::sendAckCommand()
{
    amqp_basic_ack_t s;

    memset(&s, 0, sizeof(s));
    s.delivery_tag = delivery_tag;
    s.multiple = (AMQP_MULTIPLE & parms) ? 1:0;

    amqp_send_method(*cnn, channelNum, AMQP_BASIC_ACK_METHOD, &s);
}

} // namespace
