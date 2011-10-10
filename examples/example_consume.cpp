#include <signal.h>
#include <errno.h>

#include "AMQPcpp.h"

using namespace amqpcpp;

static int i;
static AMQPQueue *q;

void
signal_handler(int)
{
    q->Cancel(q->getConsumerTag());
}

int
onCancel(AMQPMessage *message, void*)
{
    std::cout << "onCancel method called" << std::endl;

    if (message) {
        std::cout << "Cancel tag=" << message->getDeliveryTag() << std::endl;
    }
    
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

    uint32_t delivery_tag = message->getDeliveryTag();
    AMQPQueue *queue = message->getQueue();

    std::cout << *msg << " #" << i << ", tag = " << delivery_tag << std::endl;

    std::cout << "Content-type: " << message->getHeader("Content-type") << std::endl;
    std::cout << "Encoding: "<< message->getHeader("Content-encoding") << std::endl;
    std::cout << "Delivery-mode: " << message->getHeader("Delivery-mode") << std::endl;

    std::cout << "========================" << std::endl;

    queue->Ack(delivery_tag);

    /*
    if (i > 10) {
        AMQPQueue *q = message->getQueue();
        std::cout << "Consumer tag: " << message->getConsumerTag() << std::endl;
        q->Cancel(message->getConsumerTag());
    }
    */

    return 0;
};

int
main(int argc, char **argv)
{
    std::string user = "guest";
    std::string password = "guest";
    std::string host = "127.0.0.1";
    std::string port = "5672";
    std::string vhost = "/";
    std::string queue = "amqpcpp_example_queue";
    std::string exchange = "amq.direct";
    std::string key = "amqpcpp_example_key";

    int opt;

    while ((opt = getopt(argc, argv, "u:p:h:P:v:q:e:k:")) != -1) {
        switch (opt) {
            case 'u':
                user = optarg;
                break;
            case 'p':
                password = optarg;
                break;
            case 'h':
                host = optarg;
                break;
            case 'P':
                port = optarg;
                break;
            case 'v':
                vhost = optarg;
                break;
            case 'q':
                queue = optarg;
                break;
            case 'e':
                exchange = optarg;
                break;
            case 'k':
                key = optarg;
                break;
        }
    }

    // "password:user@host:portvhost");
    std::string credentials = password + ":" + user + "@" +
        host + ":" + port + vhost;

    try {
        AMQP amqp(credentials);

        q = amqp.createQueue();

        q->Declare(queue, AMQP_DURABLE);
        q->Bind(exchange, key);
        q->setConsumerTag("tag_123");

        std::string msg = "AMQPCPP example message";

        q->addEvent(AMQP_MESSAGE, onMessage, &msg);
        q->addEvent(AMQP_CANCEL, onCancel, &msg);

        signal(SIGINT, signal_handler);
        
        q->Consume();

    } catch (const AMQPException &e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }

    return 0;
}
