#include "AMQPcpp.h"

namespace amqpcpp {

AMQPBase::~AMQPBase()
{
    closeChannel();
}

void AMQPBase::checkClosed(amqp_rpc_reply_t *res)
{
    if (res->reply_type == AMQP_RESPONSE_SERVER_EXCEPTION &&
            res->reply.id == AMQP_CHANNEL_CLOSE_METHOD) {
        opened = 0;
    }
}

void AMQPBase::openChannel()
{
    amqp_channel_open(*cnn, channelNum);
    amqp_rpc_reply_t res = amqp_get_rpc_reply(*cnn);
    THROW_AMQP_EXC_IF_FAILED(res, "open channel");
    opened = 1;
}

void AMQPBase::closeChannel()
{
    if (opened) {
        amqp_channel_close(*cnn, channelNum, AMQP_REPLY_SUCCESS);
        amqp_rpc_reply_t res = amqp_get_rpc_reply(*cnn);
        THROW_AMQP_EXC_IF_FAILED(res, "close channel");
        opened = 0;
    }
}

void AMQPBase::reopen()
{
    if (opened) {
        return;
    }

    AMQPBase::openChannel();
}

int AMQPBase::getChannelNum()
{
    return channelNum;
}

void AMQPBase::setParam(short param)
{
    this->parms = param;
}

std::string AMQPBase::getName()
{
    if (!name.size()) {
        name = "";
    }

    return name;
}

void AMQPBase::setName(const std::string &name)
{
    this->name = name;
}

} // namespace
