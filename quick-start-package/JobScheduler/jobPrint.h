#ifndef JOBPRINT_H_
#define JOBPRINT_H_

#include "job.h"
#include <string>
#include <iostream>


namespace mple {

class JobPrint: public mple::Job {
public:
	std::string message;

	int Run(){
		std::cout << message << std::endl;
	}

	JobPrint(std::string message) {
		this->message = message;
	}

	virtual ~JobPrint(){}
	
};

} /* namespace mple */
#endif /* JOBPRINT_H_ */