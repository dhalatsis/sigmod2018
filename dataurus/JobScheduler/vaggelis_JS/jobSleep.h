#ifndef JOBSLEEP_H_
#define JOBSLEEP_H_

#include "job.h"
#include <string>
#include <iostream>
#include <thread>
#include <chrono>


namespace dataurus {

class JobSleep: public dataurus::Job {
public:
	int time_s;
	int jid;

	int Run(){
		std::cout << "Job " << jid << " is going to sleep for " <<  time_s << " secs..." << std::endl;
		std::this_thread::sleep_for(std::chrono::seconds(time_s));
		std::cout << "Job " << jid << " done!" << std::endl;
	}

	JobSleep(int job_id, int time) {
		time_s = time;
		jid = job_id;
	}

	virtual ~JobSleep(){}

};

} /* namespace mple */
#endif /* JOBPRINT_H_ */
