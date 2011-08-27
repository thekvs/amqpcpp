#include "AMQPcpp.h"

static int i;

using namespace amqpcpp;

int
onCancel(AMQPMessage * message )
{
    std::cout << "cancel tag="<< message->getDeliveryTag() << std::endl;

    return 0;
}

int
onMessage(AMQPMessage *message)
{
    const char *data = message->getMessage();

    if (data) {
        std::cout << data << std::endl;
    }

    i++;

    std::cout << std::endl;
    std::cout << "#" << i << " tag=" << message->getDeliveryTag() << " content-type:"<< message->getHeader("Content-type");
    std::cout << " encoding:"<< message->getHeader("Content-encoding") << " mode=" << message->getHeader("Delivery-mode") << std::endl;
    std::cout << "========================" << std::endl;

    /*
    if (i > 10) {
        AMQPQueue * q = message->getQueue();
        q->Cancel(message->getConsumerTag());
    }
    */

    return 0;
};


int main () {

    try {
        //		AMQP amqp("123123:akalend@localhost/private");

        AMQP amqp("guest:guest@127.0.0.1:5672/");

        AMQPQueue *queue = amqp.createQueue();

        queue->Declare("fast_check.queue", AMQP_DURABLE);
        queue->Bind("amq.direct", "fast_check.key");
        queue->setConsumerTag("tag_123");

        queue->addEvent(AMQP_MESSAGE, onMessage);
        queue->addEvent(AMQP_CANCEL, onCancel);

        queue->Consume(AMQP_NOACK);

    } catch (AMQPException e) {
        std::cout << e.getMessage() << std::endl;
    }

    return 0;

}

