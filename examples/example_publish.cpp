#include "AMQPcpp.h"

using namespace amqpcpp;

const int count = 10;

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

    // "user:password@host:portvhost");
    std::string credentials = user + ":" + password + "@" +
        host + ":" + port + vhost;

    try {
        AMQP amqp(credentials);

        AMQPExchange *ex = amqp.createExchange(exchange);
        ex->Declare(exchange, "direct", AMQP_DURABLE);

        AMQPQueue *q = amqp.createQueue();

        q->Declare(queue, AMQP_DURABLE);
        q->Bind(exchange, key);		

        std::string ss = "This is a test message";

        ex->setHeader("Delivery-mode", 2);
        ex->setHeader("Content-type", "text/text");
        ex->setHeader("Content-encoding", "UTF-8");

        for (int i = 0; i < count; i++) {
            ex->Publish(ss , key);
            sleep(1);
        }

    } catch (const AMQPException &e) {
        std::cerr << e.what() << std::endl;
    }

    return 0;
}
