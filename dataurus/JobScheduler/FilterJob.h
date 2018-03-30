#pragma once



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
