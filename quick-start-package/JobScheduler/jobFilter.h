#pragma once
#include "defn.h"

namespace dataurus
{

class jobFilter : public dataurus::Job {

public:

	jobFilter() {}
	virtual ~jobFilter() {}


	// This method should be implemented by subclasses.
	virtual int Run() = 0;

};


}  // namespace job_scheduler
