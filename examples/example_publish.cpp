#include "AMQPcpp.h"

using namespace amqpcpp;

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

    int count = 10;
    int pause = 1;
    int opt;

    while ((opt = getopt(argc, argv, "u:p:h:P:v:q:e:k:c:s:")) != -1) {
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
            case 'c':
                count = atoi(optarg);
                break;
            case 's':
                pause = atoi(optarg);
                break;
        }
    }

    // "password:user@host:portvhost");
    std::string credentials = password + ":" + user + "@" +
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
            ex->Publish(ss, key);
            sleep(pause);
        }

    } catch (const AMQPException &e) {
        std::cerr << e.what() << std::endl;
    }

    return 0;
}
