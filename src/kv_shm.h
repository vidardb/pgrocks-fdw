
#ifndef SRC_KV_SHM_H_
#define SRC_KV_SHM_H_

#include "kv_fdw.h"


SharedMem *InitSharedMem(void);

void cleanup_handler(void *arg);

void KVWorkerMain(void);


pid_t kvWorkerPid;  // in manager process


#endif /* SRC_KV_SHM_H_ */
