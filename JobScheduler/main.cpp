#include <iostream>
#include "job_scheduler.h"
#include "jobPrint.h"
#include "jobSleep.h"
#include "jobBarrier.h"
#include <vector>
#include <pthread.h>

using namespace std;
using namespace mple;

int main(void){
	cout << "<|*_*|>" << endl;

	A job scheduler with 4 threads
	JobScheduler js1(4);
	js1.Schedule(new JobSleep(1, 2));
	js1.Schedule(new JobSleep(2, 1));
	js1.Schedule(new JobPrint("Test 1"));
	js1.Schedule(new JobPrint("Test 2"));
	js1.Schedule(new JobSleep(3, 2));
	js1.Schedule(new JobSleep(4, 2));
	js1.Schedule(new JobSleep(5, 2));
	js1.Schedule(new JobSleep(6, 2));
	js1.Schedule(new JobPrint("Test 3"));
	js1.nojobleft();
	cout << "No jobs left at scheduler 1" << endl;
	js1.Schedule(new JobPrint("Test 4"));



	//A job scheduler with 20 threads
	int js2_threads = 20;
	JobScheduler js2(js2_threads);
	js2.Schedule(new JobSleep(11, 2));
	js2.Schedule(new JobSleep(12, 1));
	js2.Schedule(new JobPrint("Test 11"));
	//Set barrier
	pthread_cond_t cond_barr;
	pthread_mutex_t mtx_barr;
	pthread_cond_init(&cond_barr, NULL);
    pthread_mutex_init(&mtx_barr, NULL);
	vector<Job*> jobs;
	int count_barr = js2_threads - 1;
	for (int i = 0; i < js2_threads; ++i) {
		jobs.push_back( new JobBarrier(&cond_barr, &mtx_barr, &count_barr));
	}
	js2.Schedule(jobs);

	//TRY TO COMMENT OUT THE FOLLOWING LINE
	//js2.nojobleft(); cout << "No job left exited" << endl;

	js2.Schedule(new JobPrint("Test 12"));
	cout << "Test 12 scheduled " << endl;

	js2.Schedule(new JobSleep(13, 2));
	js2.Schedule(new JobSleep(14, 2));
	js2.Schedule(new JobSleep(15, 2));
	js2.Schedule(new JobSleep(16, 2));
	js2.Schedule(new JobPrint("Test 3"));
}