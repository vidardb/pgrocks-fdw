
#ifndef SRC_KV_SHM_H_
#define SRC_KV_SHM_H_

#include "kv_fdw.h"


ManagerSharedMem *InitManagerSharedMem(void);

void CloseManagerSharedMem(ManagerSharedMem *manager);

void KVWorkerMain(void);

void TerminateWorker(void);


pid_t kvWorkerPid;  // in manager process


#endif /* SRC_KV_SHM_H_ */
