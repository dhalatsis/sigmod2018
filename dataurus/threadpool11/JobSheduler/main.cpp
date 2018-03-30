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

class JobSleep: public Job {
 public:
  int time_s;
  int jid;

  int Run(){
    printf("Job %d is going to sleep for %d secs ...\n", jid, time_s);
    std::this_thread::sleep_for(std::chrono::seconds(time_s));
    printf("Job %d done!\n", jid);
  }

  JobSleep(int job_id, int time) {
    time_s = time;
    jid = job_id;
  }

  virtual ~JobSleep() {}
};

int main() {
  JobScheduler sch;
  sch.Init(10);

  for (int i = 0; i < 10; ++i) {
    sch.Schedule(new JobSleep(i, 2));
  }
  sch.Barrier();
  sch.Stop(false);
  sch.Destroy();
}


