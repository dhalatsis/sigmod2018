#include "create_job.h"


/*+++++++++++++++++++++++++++*/
/* CREATE RELATION FUNCTIONS */
/*+++++++++++++++++++++++++++*/

int JobCreateNonInterRel::Run() {
    /* Initialize the tuple array */
    for (size_t i = args_.low; i < args_.high; i++) {
        args_.tups[i].key     = args_.values[i];
        args_.tups[i].payload = i;
    }
}

int JobCreateInterRel::Run() {
    /* Initialize the tuple array */
    for (size_t i = args_.low; i < args_.high; i++) {
        args_.tups[i].key     = args_.values[args_.rids[i*(args_.rel_num) + args_.table_index]];
        args_.tups[i].payload = i;
    }
}
