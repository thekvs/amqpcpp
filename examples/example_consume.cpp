#include "AMQPcpp.h"

using namespace amqpcpp;

static int i;

int
onCancel(AMQPMessage *message, void*)
{
    std::cout << "cancel tag=" << message->getDeliveryTag() << std::endl;

    return 0;
}

int
onMessage(AMQPMessage *message, void *ctx)
{
    std::string *msg = static_cast<std::string*>(ctx);

    const std::string &data = message->getMessage();

    std::cout << data << std::endl;
    std::cout << std::endl;

    i++;

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
    std::string user = "guest";
    std::string password = "guest";
    std::string host = "127.0.0.1";
    std::string port = "5672";
    std::string vhost = "/";
    std::string queue = "amqpcpp_example_queue";
    std::string exchange = "amq.direct";
    std::string key = "amqpcpp_example_key";

    // "password:user@host:portvhost");
    std::string credentials = password + ":" + user + "@" +
        host + ":" + port + vhost;

    try {
        AMQP amqp(credentials);

        AMQPQueue *q = amqp.createQueue();

        q->Declare(queue, AMQP_DURABLE);
        q->Bind(exchange, key);
        q->setConsumerTag("tag_123");

        std::string msg = "AMQPCPP example message";

        q->addEvent(AMQP_MESSAGE, onMessage, &msg);
        q->addEvent(AMQP_CANCEL, onCancel, &msg);

        q->Consume(AMQP_NOACK);

    } catch (const AMQPException &e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }

    return 0;
}
