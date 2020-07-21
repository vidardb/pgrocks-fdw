
#ifndef SRC_KV_SHM_H_
#define SRC_KV_SHM_H_

#include "kv_fdw.h"


ManagerSharedMem *InitManagerSharedMem(void);

void CloseManagerSharedMem(ManagerSharedMem *manager);

void KVWorkerMain(WorkerProcOid *workerOid);

void TerminateWorker(WorkerProcOid *workerOid);


#endif /* SRC_KV_SHM_H_ */
