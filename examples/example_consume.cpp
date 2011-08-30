#include "AMQPcpp.h"

using namespace amqpcpp;

static int i;

int
onCancel(AMQPMessage *message, void *ctx)
{
    std::cout << "cancel tag="<< message->getDeliveryTag() << std::endl;

    return 0;
}

int
onMessage(AMQPMessage *message, void *ctx)
{
    std::string *msg = static_cast<std::string*>(ctx);

    const char *data = message->getMessage();

    if (data) {
        std::cout << data << std::endl;
    }

    i++;

    std::cout << std::endl;

    std::cout << *msg << " #" << i << ", tag = " << message->getDeliveryTag() << std::endl;

    std::cout << "Content-type: " << message->getHeader("Content-type") << std::endl;
    std::cout << "Encoding: "<< message->getHeader("Content-encoding") << std::endl;
    std::cout << "Delivery-mode: " << message->getHeader("Delivery-mode") << std::endl;
    std::cout << "========================" << std::endl;

    /*
    if (i > 10) {
        AMQPQueue * q = message->getQueue();
        q->Cancel(message->getConsumerTag());
    }
    */

    return 0;
};

int
main()
{
    try {
        AMQP amqp("guest:guest@127.0.0.1:5672/");

        AMQPQueue *queue = amqp.createQueue();

        queue->Declare("fast_check.queue", AMQP_DURABLE);
        queue->Bind("amq.direct", "fast_check.key");
        queue->setConsumerTag("tag_123");

        std::string msg = "Message";

        queue->addEvent(AMQP_MESSAGE, onMessage, &msg);
        queue->addEvent(AMQP_CANCEL, onCancel, &msg);

        queue->Consume(AMQP_NOACK);

    } catch (const AMQPException &e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }

    return 0;
}
