/*
 * year:2018 month:03 day:30
 *
 * The author disclaims copyright to this source code.
 * In place of a legal notice, here is a blessing
 * as indicated at the SQLite's source code:
 *
 *    May you do good and not evil.
 *    May you find forgiveness for yourself and forgive others.
 *    May you share freely, never taking more than you give.
 *
 * **********************************************************************
 */

#include "job_scheduler.h"

#include <string>
#include <iostream>
#include <thread>
#include <chrono>
#include <sys/time.h>
#include <time.h>

class JobSleep: public Job {
 public:
  int time_s;
  int jid;

  int Run(){
    time_s = rand() % 4;
    printf("Job %d is going to sleep for %d secs ...\n", jid, time_s);
    std::this_thread::sleep_for(std::chrono::seconds(time_s));
    printf("Job %d done!\n", jid);
    /*
    int sum = 0;
    for (int i = 0; i < 2; ++i) {
      sum += 1;
    }
    */
  }

  JobSleep(int job_id, int time) {
    time_s = time;
    jid = job_id;
  }

  virtual ~JobSleep() {}
};

int main() {
  struct timeval startt, endt;
  JobScheduler sch;
  sch.Init(8);

  gettimeofday(&startt, NULL);
  for (int i = 0; i < 10; ++i) {
    sch.Schedule(new JobSleep(i, 0));
  }
  sch.Barrier();
  gettimeofday(&endt, NULL);
  printf ("It took me %lf seconds).\n",
          (endt.tv_sec - startt.tv_sec) +
          (endt.tv_usec - startt.tv_usec) / 1000000.0);
  
  
  sch.Stop(false);
  sch.Destroy();
}


