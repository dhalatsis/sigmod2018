#include "jobBarrier.h"
namespace dataurus {

int JobBarrier::Run() {
	pthread_mutex_lock(mtx_wait);
	if((*count) > 0){
		(*count)-=1;
		pthread_cond_wait(cond_wait, mtx_wait);
	}else{
		pthread_cond_broadcast(cond_wait);
	}
	pthread_mutex_unlock(mtx_wait);

}
JobBarrier::~JobBarrier() {
	// TODO Auto-generated destructor stub
}

} /* namespace dataurus */
