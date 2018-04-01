#include "checksum_job.h"


/*+++++++++++++++++++++++++++++++++++++++*/
/* NON INTERMEDIATE CHECKSUMS FUNCTIONS  */
/*+++++++++++++++++++++++++++++++++++++++*/

int JobCheckSumInterInter::Run () {
    chainedtuplebuffer_t * cb = args_.cb;

    /* Get some variables form the chained buffer */
    tuplebuffer_t * tb = cb->buf;
    uint32_t   numbufs = cb->numbufs;
    uint64_t * csums   = args_.priv_checsums;
    unsigned * rids    = args_.rids_r;
    unsigned   relnum  = args_.rel_num_r;

    /* Get the checksums for table R */
    int index = 0;
    tuple_t * tups = tb->tuples;
    for (struct checksumST & p: *(args_.distinctPairs_r)) {
        unsigned   idx    = p.index;
        uint64_t * values = p.values;

        /* Loop the 2d array to find checksums */
        for (size_t i = 0; i < cb->writepos; i++) {
            csums[index] += values[rids[(tups[i].key)*relnum + idx]];
        }
        index++;
    }

    /* Get the checksums for table S */
    rids   = args_.rids_s;
    relnum = args_.rel_num_s;
    for (struct checksumST & p: *(args_.distinctPairs_s)) {
        unsigned   idx    = p.index;
        uint64_t * values = p.values;

        /* Loop the 2d array to find checksums */
        for (size_t i = 0; i < cb->writepos; i++) {
            csums[index] += values[rids[(tups[i].payload)*relnum + idx]];
        }
        index++;
    }

    if (numbufs == 1) return 1;

    /* Run the other buffers: Usually not happening */
    tb = tb->next;
    for (uint32_t buf_i = 0; buf_i < numbufs - 1; buf_i++) {

        relnum = args_.rel_num_r;
        rids   = args_.rids_r;
        tups   = tb->tuples;
        index  = 0;
        for (struct checksumST & p: *(args_.distinctPairs_r)) {
            unsigned   idx    = p.index;
            uint64_t * values = p.values;

            /* Loop the 2d array to find checksums */
            for (size_t i = 0; i < CHAINEDBUFF_NUMTUPLESPERBUF; i++) {
                csums[index] += values[rids[(tups[i].key)*relnum + idx]];
            }
            index++;
        }

        relnum = args_.rel_num_s;
        rids   = args_.rids_s;
        for (struct checksumST & p: *(args_.distinctPairs_s)) {
            unsigned   idx    = p.index;
            uint64_t * values = p.values;

            /* Loop the 2d array to find checksums */
            for (size_t i = 0; i < CHAINEDBUFF_NUMTUPLESPERBUF; i++) {
                csums[index] += values[rids[(tups[i].payload)*relnum + idx]];
            }
            index++;
        }

        /* Go the the next buffer */
        tb = tb->next;
    }
}


int JobCheckSumInterNonInter::Run () {
    chainedtuplebuffer_t * cb = args_.cb;

    /* Get some variables form the chained buffer */
    tuplebuffer_t * tb = cb->buf;
    uint32_t   numbufs = cb->numbufs;
    uint64_t * csums   = args_.priv_checsums;
    unsigned * rids    = args_.rids_r;
    unsigned   relnum  = args_.rel_num_r;

    /* Get the checksums for table R */
    int index = 0;
    tuple_t * tups = tb->tuples;
    for (struct checksumST & p: *(args_.distinctPairs_r)) {
        unsigned   idx    = p.index;
        uint64_t * values = p.values;

        /* Loop the 2d array to find checksums */
        for (size_t i = 0; i < cb->writepos; i++) {
            csums[index] += values[rids[(tups[i].key)*relnum + idx]];
        }
        index++;
    }

    /* Get the checksums for table S */
    for (struct checksumST & p: *(args_.distinctPairs_s)) {
        unsigned   idx    = p.index;
        uint64_t * values = p.values;

        /* Loop the 2d array to find checksums */
        for (size_t i = 0; i < cb->writepos; i++) {
            csums[index] += values[tups[i].payload];
        }
        index++;
    }

    if (numbufs == 1) return 1;

    /* Run the other buffers: Usually not happening */
    tb = tb->next;
    for (uint32_t buf_i = 0; buf_i < numbufs - 1; buf_i++) {

        tups   = tb->tuples;
        index  = 0;
        for (struct checksumST & p: *(args_.distinctPairs_r)) {
            unsigned   idx    = p.index;
            uint64_t * values = p.values;

            /* Loop the 2d array to find checksums */
            for (size_t i = 0; i < CHAINEDBUFF_NUMTUPLESPERBUF; i++) {
                csums[index] += values[rids[(tups[i].key)*relnum + idx]];
            }
            index++;
        }

        for (struct checksumST & p: *(args_.distinctPairs_s)) {
            unsigned   idx    = p.index;
            uint64_t * values = p.values;

            /* Loop the 2d array to find checksums */
            for (size_t i = 0; i < CHAINEDBUFF_NUMTUPLESPERBUF; i++) {
                csums[index] += values[tups[i].payload];
            }
            index++;
        }

        /* Go the the next buffer */
        tb = tb->next;
    }
}

// R no inter  S inter
int JobCheckSumNonInterInter::Run () {
    chainedtuplebuffer_t * cb = args_.cb;

    /* Get some variables form the chained buffer */
    tuplebuffer_t * tb = cb->buf;
    uint32_t   numbufs = cb->numbufs;
    uint64_t * csums   = args_.priv_checsums;
    unsigned * rids    = args_.rids_s;
    unsigned   relnum  = args_.rel_num_s;

    /* Get the checksums for table R */
    int index = 0;
    tuple_t * tups = tb->tuples;
    for (struct checksumST & p: *(args_.distinctPairs_r)) {
        unsigned   idx    = p.index;
        uint64_t * values = p.values;

        /* Loop the 2d array to find checksums */
        for (size_t i = 0; i < cb->writepos; i++) {
            csums[index] += values[tups[i].key];
        }
        index++;
    }

    /* Get the checksums for table S */
    for (struct checksumST & p: *(args_.distinctPairs_s)) {
        unsigned   idx    = p.index;
        uint64_t * values = p.values;

        /* Loop the 2d array to find checksums */
        for (size_t i = 0; i < cb->writepos; i++) {
            csums[index] += values[rids[(tups[i].payload)*relnum + idx]];
        }
        index++;
    }

    if (numbufs == 1) return 1;

    /* Run the other buffers: Usually not happening */
    tb = tb->next;
    for (uint32_t buf_i = 0; buf_i < numbufs - 1; buf_i++) {

        tups   = tb->tuples;
        index  = 0;
        for (struct checksumST & p: *(args_.distinctPairs_r)) {
            unsigned   idx    = p.index;
            uint64_t * values = p.values;

            /* Loop the 2d array to find checksums */
            for (size_t i = 0; i < CHAINEDBUFF_NUMTUPLESPERBUF; i++) {
                csums[index] += values[tups[i].key];
            }
            index++;
        }

        for (struct checksumST & p: *(args_.distinctPairs_s)) {
            unsigned   idx    = p.index;
            uint64_t * values = p.values;

            /* Loop the 2d array to find checksums */
            for (size_t i = 0; i < CHAINEDBUFF_NUMTUPLESPERBUF; i++) {
                csums[index] += values[rids[(tups[i].payload)*relnum + idx]];
            }
            index++;
        }

        /* Go the the next buffer */
        tb = tb->next;
    }
}

// R no inter  S non inter
int JobCheckSumNonInterNonInter::Run () {
    chainedtuplebuffer_t * cb = args_.cb;

    /* Get some variables form the chained buffer */
    tuplebuffer_t * tb = cb->buf;
    uint32_t   numbufs = cb->numbufs;
    uint64_t * csums   = args_.priv_checsums;

    /* Get the checksums for table R */
    int index = 0;
    tuple_t * tups = tb->tuples;
    for (struct checksumST & p: *(args_.distinctPairs_r)) {
        unsigned   idx    = p.index;
        uint64_t * values = p.values;

        /* Loop the 2d array to find checksums */
        for (size_t i = 0; i < cb->writepos; i++) {
            csums[index] += values[tups[i].key];
        }
        index++;
    }

    /* Get the checksums for table S */
    for (struct checksumST & p: *(args_.distinctPairs_s)) {
        unsigned   idx    = p.index;
        uint64_t * values = p.values;

        /* Loop the 2d array to find checksums */
        for (size_t i = 0; i < cb->writepos; i++) {
            csums[index] += values[tups[i].payload];
        }
        index++;
    }

    if (numbufs == 1) return 1;

    /* Run the other buffers: Usually not happening */
    tb = tb->next;
    for (uint32_t buf_i = 0; buf_i < numbufs - 1; buf_i++) {

        tups   = tb->tuples;
        index  = 0;
        for (struct checksumST & p: *(args_.distinctPairs_r)) {
            unsigned   idx    = p.index;
            uint64_t * values = p.values;

            /* Loop the 2d array to find checksums */
            for (size_t i = 0; i < CHAINEDBUFF_NUMTUPLESPERBUF; i++) {
                csums[index] += values[tups[i].key];
            }
            index++;
        }

        for (struct checksumST & p: *(args_.distinctPairs_s)) {
            unsigned   idx    = p.index;
            uint64_t * values = p.values;

            /* Loop the 2d array to find checksums */
            for (size_t i = 0; i < CHAINEDBUFF_NUMTUPLESPERBUF; i++) {
                csums[index] += values[tups[i].payload];
            }
            index++;
        }

        /* Go the the next buffer */
        tb = tb->next;
    }
}
