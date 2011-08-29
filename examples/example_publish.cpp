#include "AMQPcpp.h"

int main (int argc, char** argv) {

	using namespace amqpcpp;

	try {
//		AMQP amqp;
//		AMQP amqp(AMQPDEBUG);
	
		AMQP amqp("123123:akalend@localhost:5673/private");		// all connect std::string

		AMQPExchange * ex = amqp.createExchange("e");
		ex->Declare("e", "fanout");

		AMQPQueue * qu2 = amqp.createQueue("q2");
		qu2->Declare();
		qu2->Bind( "e", "");		

		std::string ss = "message 1 ";
		/* test very long message
		ss = ss+ss+ss+ss+ss+ss+ss;
		ss += ss+ss+ss+ss+ss+ss+ss;
		ss += ss+ss+ss+ss+ss+ss+ss;
		ss += ss+ss+ss+ss+ss+ss+ss;
		ss += ss+ss+ss+ss+ss+ss+ss;
*/

		ex->setHeader("Delivery-mode", 2);
		ex->setHeader("Content-type", "text/text");
		ex->setHeader("Content-encoding", "UTF-8");

		ex->Publish(  ss , ""); // publish very long message
		
		ex->Publish(  "message 2 " , "");
		ex->Publish(  "message 3 " , "");
		
		
		if (argc==2) {
			AMQPQueue * qu = amqp.createQueue();			
			qu->Cancel(   amqp_cstring_bytes(argv[1]) );
		}												
						
	} catch (const AMQPException &e) {
		std::cerr << e.what() << std::endl;
	}

	return 0;

}
