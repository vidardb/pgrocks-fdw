
#ifndef SRC_KV_SHM_H_
#define SRC_KV_SHM_H_

#include "kv_fdw.h"


pid_t kvWorkerPid;  // in postmaster process

/*
 * referenced by thread of postmaster process, client process, worker process
 */
char *ResponseQueue[RESPONSEQUEUELENGTH];

/*
 * Initialize shared memory for responses
 * called by the thread in postmaster process
 */
void InitResponseArea(void);

void cleanup_handler(void *arg);

int StartKVWorker(void);


#endif /* SRC_KV_SHM_H_ */
