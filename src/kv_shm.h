
#ifndef SRC_KV_SHM_H_
#define SRC_KV_SHM_H_

#include "kv_fdw.h"


ManagerShm *InitManagerShm(void);

void CloseManagerShm(ManagerShm *manager);

void KVWorkerMainOld(WorkerProcKey *workerKey);

void TerminateWorker(WorkerProcKey *workerKey);


#endif /* SRC_KV_SHM_H_ */
