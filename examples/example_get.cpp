#include "AMQPcpp.h"

int main () {

	using namespace amqpcpp;

	try {
//		AMQP amqp("123123:akalend@localhost/private");
		AMQP amqp("123123:akalend@localhost:5673/private");		

		AMQPQueue * qu2 = amqp.createQueue("q2");
		qu2->Declare();		
		
		
		while (  1 ) {
			qu2->Get(AMQP_NOACK);

			AMQPMessage * m= qu2->getMessage();
			
			std::cout << "count: "<<  m->getMessageCount() << std::endl;											 
			if (m->getMessageCount() > -1) {
			
			std::cout << "message\n"<< m->getMessage() << "\nmessage key: "<<  m->getRoutingKey() << std::endl;
			std::cout << "exchange: "<<  m->getExchange() << std::endl;											
			std::cout << "Content-type: "<< m->getHeader("Content-type") << std::endl;	
			std::cout << "Content-encoding: "<< m->getHeader("Content-encoding") << std::endl;	
			} else 
				break;				
						
							
		}	
	} catch (const AMQPException &e) {
		std::cerr << e.what() << std::endl;
	}

	return 0;					

}
