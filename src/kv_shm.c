
#include "kv_shm.h"
#include "kv_storage.h"
#include "kv_posix.h"

#include <fcntl.h>

#include "postmaster/fork_process.h"
#include "postmaster/postmaster.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "utils/ps_status.h"
#include "utils/hsearch.h"
#include "postmaster/bgworker.h"


typedef struct KVHashEntry {
    Oid relationId;
    uint32 ref;
    void *db;
} KVHashEntry;

typedef struct KVTableProcOpHashKey {
    Oid relationId;
    pid_t pid;
    uint64 operationId;
} KVTableProcOpHashKey;

typedef struct KVIterHashEntry {
    KVTableProcOpHashKey key;
    void *iter;
} KVIterHashEntry;

#ifdef VIDARDB
typedef struct KVReadOptionsEntry {
    KVTableProcOpHashKey key;
    void *readOptions;
    void *range;
} KVReadOptionsEntry;

static HTAB *kvReadOptionsHash = NULL;
#endif


/*
 * reference by worker process
 */
static HTAB *kvTableHash = NULL;

static HTAB *kvIterHash = NULL;

/* non-shared hash can be enlarged */
static long HASHSIZE = 1;

/*
 * referenced by worker process, backend process
 */
static char *ResponseQueue[RESPONSEQUEUELENGTH];


static void OpenResponse(char *area);

static void CloseResponse(char *area);

static void CountResponse(char *area);

static void GetIterResponse(char *area);

static void DelIterResponse(char *area);

static void ReadBatchResponse(char *area);

static void GetResponse(char *area);

static void PutResponse(char *area);

static void LoadResponse(char *area);

static void DeleteResponse(char *area);

#ifdef VIDARDB
static void RangeQueryResponse(char *area);

static void ClearRangeQueryMetaResponse(char *area);
#endif


/*
 * A request process must acquire the mutex of the shared memory before calling
 * this functions, so processes check the available response slot in the FIFO
 * manner. If all the response slots are used by other processes, the caller
 * process will loop here. Called by manager process and backend process.
 */
static inline uint32 GetResponseQueueIndex(WorkerSharedMem *worker) {
    while (true) {
        for (uint32 i = 0; i < RESPONSEQUEUELENGTH; i++) {
            int ret = SemTryWait(&worker->responseMutex[i], __func__);
            if (ret == 0) {
                return i;
            }
        }
    }
}

/*
 * Initialize shared memory for responses
 * called by worker process
 */
static void InitResponseArea(Oid databaseId) {
    for (uint32 i = 0; i < RESPONSEQUEUELENGTH; i++) {
        char filename[FILENAMELENGTH];
        snprintf(filename, FILENAMELENGTH, "%s%d%u", RESPONSEFILE, i, databaseId);
        ShmUnlink(filename, __func__);
        int fd = ShmOpen(filename,
                         O_CREAT | O_RDWR | O_EXCL,
                         PERMISSION,
                         __func__);
        Ftruncate(fd, BUFSIZE, __func__);
        ResponseQueue[i] = Mmap(NULL, 
                                BUFSIZE,
                                PROT_READ | PROT_WRITE,
                                MAP_SHARED,
                                fd,
                                0,
                                __func__);
        Fclose(fd, __func__);
    }
}

/*
 * Open shared memory for responses
 * called by backend process
 */
static void OpenResponseArea(Oid databaseId) {
    for (uint32 i = 0; i < RESPONSEQUEUELENGTH; i++) {
        if (ResponseQueue[i] == NULL) {
            char filename[FILENAMELENGTH];
            snprintf(filename, FILENAMELENGTH, "%s%d%u", RESPONSEFILE, i, databaseId);
            int fd = ShmOpen(filename, O_RDWR, PERMISSION, __func__);
            ResponseQueue[i] = Mmap(NULL,
                                    BUFSIZE,
                                    PROT_READ | PROT_WRITE,
                                    MAP_SHARED,
                                    fd,
                                    0,
                                    __func__);
            Fclose(fd, __func__);
        }
    }
}

/*
 * Compare function for KVIterHash
 */
static inline int CompareKVTableProcOpHashKey(const void *key1,
                                              const void *key2,
                                              Size keysize) {
    const KVTableProcOpHashKey *k1 = (const KVTableProcOpHashKey *)key1;
    const KVTableProcOpHashKey *k2 = (const KVTableProcOpHashKey *)key2;

    if (k1 == NULL || k2 == NULL) {
        return -1;
    }

    if (k1->relationId == k2->relationId &&
        k1->pid == k2->pid &&
        k1->operationId == k2->operationId) {
        return 0;
    }

    return -1;
}

/*
 * Initialize manager shared memory
 */
ManagerSharedMem *InitManagerSharedMem() {
    ManagerSharedMem *manager = NULL;

    ShmUnlink(BACKFILE, __func__);
    int fd = ShmOpen(BACKFILE, O_CREAT | O_RDWR | O_EXCL, PERMISSION, __func__);
    Ftruncate(fd, sizeof(*manager), __func__);
    manager = Mmap(NULL,
                   sizeof(*manager),
                   PROT_READ | PROT_WRITE,
                   MAP_SHARED,
                   fd,
                   0,
                   __func__);
    Fclose(fd, __func__);

    SemInit(&manager->mutex, 1, 1, __func__);
    SemInit(&manager->manager, 1, 0, __func__);
    SemInit(&manager->backend, 1, 0, __func__);
    SemInit(&manager->ready, 1, 0, __func__);

    manager->databaseId = InvalidOid;

    return manager;
}

void CloseManagerSharedMem(ManagerSharedMem *manager) {
    SemDestroy(&manager->mutex, __func__);
    SemDestroy(&manager->manager, __func__);
    SemDestroy(&manager->backend, __func__);
    SemDestroy(&manager->ready, __func__);

    Munmap(manager, sizeof(*manager), __func__);
    ShmUnlink(BACKFILE, __func__);
}

/* called by manager process to terminate worker process */
void TerminateWorker(Oid databaseId) {
    char filename[FILENAMELENGTH];
    snprintf(filename, FILENAMELENGTH, "%s%u", BACKFILE, databaseId);
    int fd = ShmOpen(filename, O_RDWR, PERMISSION, __func__);
    WorkerSharedMem *worker = Mmap(NULL,
                                   sizeof(*worker),
                                   PROT_READ | PROT_WRITE,
                                   MAP_SHARED,
                                   fd,
                                   0,
                                   __func__);
    Fclose(fd, __func__);

    FuncName func = TERMINATE;
    SemWait(&worker->mutex, __func__);
    SemWait(&worker->full, __func__);
    memcpy(worker->area, &func, sizeof(func));
    uint32 responseId = GetResponseQueueIndex(worker);
    memcpy(worker->area + sizeof(func), &responseId, sizeof(responseId));
    SemPost(&worker->worker, __func__);
    SemWait(&worker->responseSync[responseId], __func__);
    SemPost(&worker->mutex, __func__);

    Munmap(worker, sizeof(*worker), __func__);
}

/*
 * Initialize worker shared memory
 */
static WorkerSharedMem *InitWorkerSharedMem(Oid databaseId) {
    char filename[FILENAMELENGTH];
    snprintf(filename, FILENAMELENGTH, "%s%u", BACKFILE, databaseId);

    WorkerSharedMem *worker = NULL;
    ShmUnlink(filename, __func__);
    int fd = ShmOpen(filename, O_CREAT | O_RDWR | O_EXCL, PERMISSION, __func__);
    Ftruncate(fd, sizeof(*worker), __func__);
    worker = Mmap(NULL,
                  sizeof(*worker),
                  PROT_READ | PROT_WRITE,
                  MAP_SHARED,
                  fd,
                  0,
                  __func__);
    Fclose(fd, __func__);

    /* Initialize the response area */
    InitResponseArea(databaseId);

    SemInit(&worker->mutex, 1, 1, __func__);
    SemInit(&worker->full, 1, 1, __func__);
    SemInit(&worker->worker, 1, 0, __func__);

    for (uint32 i = 0; i < RESPONSEQUEUELENGTH; i++) {
        SemInit(&worker->responseMutex[i], 1, 1, __func__);
        SemInit(&worker->responseSync[i], 1, 0, __func__);
    }

    return worker;
}

static void CloseWorkerSharedMem(WorkerSharedMem *worker, Oid databaseId) {
    // release the response area first
    for (uint32 i = 0; i < RESPONSEQUEUELENGTH; i++) {
        Munmap(ResponseQueue[i], BUFSIZE, __func__);
        char filename[FILENAMELENGTH];
        snprintf(filename, FILENAMELENGTH, "%s%d%u", RESPONSEFILE, i, databaseId);
        ShmUnlink(filename, __func__);
    }

    SemDestroy(&worker->mutex, __func__);
    SemDestroy(&worker->full, __func__);
    SemDestroy(&worker->worker, __func__);

    for (uint32 i = 0; i < RESPONSEQUEUELENGTH; i++) {
        SemDestroy(&worker->responseMutex[i], __func__);
        SemDestroy(&worker->responseSync[i], __func__);
    }

    Munmap(worker, sizeof(*worker), __func__);
    char filename[FILENAMELENGTH];
    snprintf(filename, FILENAMELENGTH, "%s%u", BACKFILE, databaseId);
    ShmUnlink(filename, __func__);
}

/*
 * Main loop for the worker process.
 */
void KVWorkerMain(Oid databaseId) {
    ereport(DEBUG1, (errmsg("KVWorker started")));

    /* first init the worker specific shared mem */
    WorkerSharedMem *worker = InitWorkerSharedMem(databaseId);

    /* build channel with manager to notify init is done */
    int fd = ShmOpen(BACKFILE, O_RDWR, PERMISSION, __func__);
    ManagerSharedMem *manager = Mmap(NULL,
                                     sizeof(*manager),
                                     PROT_READ | PROT_WRITE,
                                     MAP_SHARED,
                                     fd,
                                     0,
                                     __func__);
    Fclose(fd, __func__);
    SemPost(&manager->ready, __func__);

    /* Connect to our database */
    BackgroundWorkerInitializeConnectionByOid(databaseId, InvalidOid, 0);

    HASHCTL hash_ctl;
    memset(&hash_ctl, 0, sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(Oid);
    hash_ctl.entrysize = sizeof(KVHashEntry);
    kvTableHash = hash_create("kvTableHash",
                              HASHSIZE,
                              &hash_ctl,
                              HASH_ELEM | HASH_BLOBS);

    HASHCTL iter_hash_ctl;
    memset(&iter_hash_ctl, 0, sizeof(iter_hash_ctl));
    iter_hash_ctl.keysize = sizeof(KVTableProcOpHashKey);
    iter_hash_ctl.entrysize = sizeof(KVIterHashEntry);
    iter_hash_ctl.match = CompareKVTableProcOpHashKey;
    kvIterHash = hash_create("kvIterHash",
                             HASHSIZE,
                             &iter_hash_ctl,
                             HASH_ELEM | HASH_COMPARE);

    #ifdef VIDARDB
    HASHCTL option_hash_ctl;
    memset(&option_hash_ctl, 0, sizeof(option_hash_ctl));
    option_hash_ctl.keysize = sizeof(KVTableProcOpHashKey);
    option_hash_ctl.entrysize = sizeof(KVReadOptionsEntry);
    option_hash_ctl.match = CompareKVTableProcOpHashKey;
    kvReadOptionsHash = hash_create("kvReadOptionsHash",
                                    HASHSIZE,
                                    &option_hash_ctl,
                                    HASH_ELEM | HASH_COMPARE);
    #endif

    char buf[BUFSIZE];
    do {
        SemWait(&worker->worker, __func__);

        FuncName func;
        memcpy(&func, worker->area, sizeof(func));
        uint32 responseId;
        memcpy(&responseId, worker->area + sizeof(func), sizeof(responseId));

        memset(buf, 0, BUFSIZE);
        memcpy(buf, worker->area + sizeof(func), BUFSIZE - sizeof(func));
        SemPost(&worker->full, __func__);

        if (func == TERMINATE) {
            SemPost(&worker->responseSync[responseId], __func__);
            break;
        }

        switch (func) {
            case OPEN:
                OpenResponse(buf + sizeof(responseId));
                break;
            case CLOSE:
                CloseResponse(buf + sizeof(responseId));
                break;
            case COUNT:
                CountResponse(buf);
                break;
            case GETITER:
                GetIterResponse(buf + sizeof(responseId));
                break;
            case DELITER:
                DelIterResponse(buf + sizeof(responseId));
                break;
            case READBATCH:
                ReadBatchResponse(buf);
                break;
            case GET:
                GetResponse(buf);
                break;
            case PUT:
                PutResponse(buf + sizeof(responseId));
                break;
            case DELETE:
                DeleteResponse(buf + sizeof(responseId));
                break;
            #ifdef VIDARDB
            case RANGEQUERY:
                RangeQueryResponse(buf);
                break;
            case CLEARRQMETA:
                ClearRangeQueryMetaResponse(buf + sizeof(responseId));
                break;
            #endif
            case LOAD:
                SemPost(&worker->responseSync[responseId], __func__);
                LoadResponse(buf + sizeof(responseId));
                break;
            default:
                ereport(ERROR, (errmsg("%s failed in switch", __func__)));
        }

        if (func != LOAD) {
            SemPost(&worker->responseSync[responseId], __func__);
        }
    } while (true);

    HASH_SEQ_STATUS status;
    hash_seq_init(&status, kvTableHash);
    KVHashEntry *entry = NULL;
    while ((entry = hash_seq_search(&status)) != NULL) {
        printf("\n ref count %d\n", entry->ref);
        Close(entry->db);
    }

    CloseWorkerSharedMem(worker, databaseId);

    ereport(DEBUG1, (errmsg("KVWorker shutting down")));
}

WorkerSharedMem *OpenRequest(Oid relationId,
                             ManagerSharedMem **managerPtr,
                             WorkerSharedMem *worker, ...) {
//    printf("\n============%s============\n", __func__);

    ManagerSharedMem *manager = *managerPtr;
    if (!manager) {
        /*
         * backend process talks to manager about worker info
         */
        int fd = ShmOpen(BACKFILE, O_RDWR, PERMISSION, __func__);
        manager = *managerPtr = Mmap(NULL,
                                     sizeof(*manager),
                                     PROT_READ | PROT_WRITE,
                                     MAP_SHARED,
                                     fd,
                                     0,
                                     __func__);
        Fclose(fd, __func__);

        /*
         * Lock among child processes.
         * Manager only serves one backend process.
         */
        SemWait(&manager->mutex, __func__);

        manager->databaseId = MyDatabaseId;
        SemPost(&manager->manager, __func__);
        SemWait(&manager->backend, __func__);

        /* unlock */
        SemPost(&manager->mutex, __func__);
    }

    if (!worker) {
        /*
         * backend process talks to worker and do the real work.
         * Each worker has its own set of shared memory.
         */
        char filename[FILENAMELENGTH];
        snprintf(filename, FILENAMELENGTH, "%s%u", BACKFILE, MyDatabaseId);
        int fd = ShmOpen(filename, O_RDWR, PERMISSION, __func__);
        worker = Mmap(NULL,
                      sizeof(*worker),
                      PROT_READ | PROT_WRITE,
                      MAP_SHARED,
                      fd,
                      0,
                      __func__);
        Fclose(fd, __func__);

        OpenResponseArea(MyDatabaseId);
    }

    SemWait(&worker->mutex, __func__);

    /* wait for the worker to copy out the previous request */
    SemWait(&worker->full, __func__);

    char *current = worker->area;
    FuncName func = OPEN;
    memcpy(current, &func, sizeof(func));
    current += sizeof(func);

    uint32 responseId = GetResponseQueueIndex(worker);
    memcpy(current, &responseId, sizeof(responseId));
    current += sizeof(responseId);

    #ifdef VIDARDB
    va_list vl;
    va_start(vl, worker);

    bool useColumn = (bool) va_arg(vl, int);
    memcpy(current, &useColumn, sizeof(useColumn));
    current += sizeof(useColumn);

    int attrCount = va_arg(vl, int);
    memcpy(current, &attrCount, sizeof(attrCount));
    current += sizeof(attrCount);

    ComparatorOptions* opts = va_arg(vl, ComparatorOptions*);
    memcpy(current, opts, sizeof(*opts));
    current += sizeof(*opts);

    va_end(vl);
    #endif

    KVFdwOptions *fdwOptions = KVGetOptions(relationId);
    char *path = fdwOptions->filename;
    strcpy(current, path);

    SemPost(&worker->worker, __func__);
    /* unlock */
    SemPost(&worker->mutex, __func__);

    SemWait(&worker->responseSync[responseId], __func__);
    SemPost(&worker->responseMutex[responseId], __func__);
    return worker;
}

static void OpenResponse(char *area) {
//    printf("\n============%s============\n", __func__);

    #ifdef VIDARDB
    bool *useColumn = (bool *)area;
    area += sizeof(*useColumn);

    int *attrCount = (int *)area;
    area += sizeof(*attrCount);

    ComparatorOptions *opts = (ComparatorOptions *)area;
    area += sizeof(*opts);
    #endif

    char path[PATHMAXLENGTH];
    strcpy(path, area);
    char *pos = strrchr(path, '/');
    Oid relationId = atoi(pos + 1);
    bool found;

    KVHashEntry *entry = hash_search(kvTableHash, &relationId, HASH_ENTER, &found);
    if (!found) {
        entry->relationId = relationId;
        entry->ref = 1;
        #ifdef VIDARDB
        entry->db = Open(path, *useColumn, *attrCount, opts);
        #else
        entry->db = Open(path);
        #endif
    } else {
        entry->ref++;
//        printf("\n%s ref %d\n", __func__, entry->ref);
    }
}

void CloseRequest(Oid relationId, WorkerSharedMem *worker) {
//    printf("\n============%s============\n", __func__);

    SemWait(&worker->mutex, __func__);
    SemWait(&worker->full, __func__);

    char *current = worker->area;
    FuncName func = CLOSE;
    memcpy(current, &func, sizeof(func));
    current += sizeof(func);

    uint32 responseId = GetResponseQueueIndex(worker);
    memcpy(current, &responseId, sizeof(responseId));
    current += sizeof(responseId);

    memcpy(current, &relationId, sizeof(relationId));

    SemPost(&worker->worker, __func__);
    SemPost(&worker->mutex, __func__);

    SemWait(&worker->responseSync[responseId], __func__);
    SemPost(&worker->responseMutex[responseId], __func__);
}

static void CloseResponse(char *area) {
//    printf("\n============%s============\n", __func__);

    Oid *relationId = (Oid *)area;

    bool found;
    KVHashEntry *entry = hash_search(kvTableHash, relationId, HASH_FIND, &found);
    if (!found) {
        ereport(ERROR, (errmsg("%s failed in hash search", __func__)));
    }

    entry->ref--;
//    printf("\n%s ref %d\n", __func__, entry->ref);
}

uint64 CountRequest(Oid relationId, WorkerSharedMem *worker) {
//    printf("\n============%s============\n", __func__);

    SemWait(&worker->mutex, __func__);
    SemWait(&worker->full, __func__);

    char *current = worker->area;
    FuncName func = COUNT;
    memcpy(current, &func, sizeof(func));
    current += sizeof(func);

    uint32 responseId = GetResponseQueueIndex(worker);
    memcpy(current, &responseId, sizeof(responseId));
    current += sizeof(responseId);

    memcpy(current, &relationId, sizeof(relationId));

    SemPost(&worker->worker, __func__);
    SemPost(&worker->mutex, __func__);

    SemWait(&worker->responseSync[responseId], __func__);
    uint64 count;
    memcpy(&count, ResponseQueue[responseId], sizeof(count));
    SemPost(&worker->responseMutex[responseId], __func__);
    return count;
}

static void CountResponse(char *area) {
//    printf("\n============%s============\n", __func__);

    uint32 *responseId = (uint32 *)area;
    area += sizeof(*responseId);

    Oid *relationId = (Oid *)area;

    bool found;
    KVHashEntry *entry = hash_search(kvTableHash, relationId, HASH_FIND, &found);
    if (!found) {
        ereport(ERROR, (errmsg("%s failed in hash search", __func__)));
    }

    uint64 count = Count(entry->db);
    memcpy(ResponseQueue[*responseId], &count, sizeof(count));
}

void GetIterRequest(Oid relationId, uint64 operationId, WorkerSharedMem *worker) {
//    printf("\n============%s============\n", __func__);

    SemWait(&worker->mutex, __func__);
    SemWait(&worker->full, __func__);

    char *current = worker->area;
    FuncName func = GETITER;
    memcpy(current, &func, sizeof(func)); 
    current += sizeof(func);

    uint32 responseId = GetResponseQueueIndex(worker);
    memcpy(current, &responseId, sizeof(responseId));
    current += sizeof(responseId);

    memcpy(current, &relationId, sizeof(relationId));
    current += sizeof(relationId);

    pid_t pid = getpid();
    memcpy(current, &pid, sizeof(pid));
    current += sizeof(pid);

    memcpy(current, &operationId, sizeof(operationId));

    SemPost(&worker->worker, __func__);
    SemPost(&worker->mutex, __func__);

    SemWait(&worker->responseSync[responseId], __func__);
    SemPost(&worker->responseMutex[responseId], __func__);
}

static void GetIterResponse(char *area) {
//    printf("\n============%s============\n", __func__);

    KVTableProcOpHashKey iterKey;
    iterKey.relationId = *((Oid *)area);
    area += sizeof(iterKey.relationId);

    iterKey.pid = *((pid_t *)area);
    area += sizeof(iterKey.pid);

    iterKey.operationId = *((uint64 *)area);

    bool found;
    KVHashEntry *entry = hash_search(kvTableHash,
                                     &iterKey.relationId,
                                     HASH_FIND,
                                     &found);

    if (!found) {
        ereport(ERROR, (errmsg("%s failed in hash search", __func__)));
    }

    bool iterFound;
    KVIterHashEntry *iterEntry = hash_search(kvIterHash,
                                             &iterKey,
                                             HASH_ENTER,
                                             &iterFound);
    if (!iterFound) {
        iterEntry->key = iterKey;
    }
    iterEntry->iter = GetIter(entry->db);
}

void DelIterRequest(Oid relationId,
                    uint64 operationId,
                    WorkerSharedMem *worker,
                    TableReadState *readState) {
//    printf("\n============%s============\n", __func__);

    if (readState->buf != NULL) {
        Munmap(readState->buf, READBATCHSIZE, __func__);
    }
    /* shared memory will be open in ReadBatchResponse anyway */
    char filename[FILENAMELENGTH];
    pid_t pid = getpid();
    snprintf(filename,
             FILENAMELENGTH,
             "%s%d%lu",
             READBATCHFILE,
             pid,
             readState->operationId);
    ShmUnlink(filename, __func__);

    SemWait(&worker->mutex, __func__);
    SemWait(&worker->full, __func__);

    char *current = worker->area;
    FuncName func = DELITER;
    memcpy(current, &func, sizeof(func));
    current += sizeof(func);

    uint32 responseId = GetResponseQueueIndex(worker);
    memcpy(current, &responseId, sizeof(responseId));
    current += sizeof(responseId);

    memcpy(current, &relationId, sizeof(relationId));
    current += sizeof(relationId);

    memcpy(current, &pid, sizeof(pid));
    current += sizeof(pid);

    memcpy(current, &operationId, sizeof(operationId));

    SemPost(&worker->worker, __func__);
    SemPost(&worker->mutex, __func__);

    SemWait(&worker->responseSync[responseId], __func__);
    SemPost(&worker->responseMutex[responseId], __func__);
}

static void DelIterResponse(char *area) {
//    printf("\n============%s============\n", __func__);

    KVTableProcOpHashKey iterKey;
    iterKey.relationId = *((Oid *)area);
    area += sizeof(iterKey.relationId);

    iterKey.pid = *((pid_t *)area);
    area += sizeof(iterKey.pid);

    iterKey.operationId = *((uint64 *)area);

    bool found;
    KVIterHashEntry *entry = hash_search(kvIterHash, &iterKey, HASH_REMOVE, &found);
    if (!found) {
        ereport(ERROR, (errmsg("%s failed in hash search", __func__)));
    }

    DelIter(entry->iter);
    /* might reuse, so must set NULL */
    entry->iter = NULL;
}

bool ReadBatchRequest(Oid relationId,
                      uint64 operationId,
                      WorkerSharedMem *worker,
                      char **buf,
                      size_t *bufLen) {
//    printf("\n============%s============\n", __func__);

    /* munmap the shared memory so that Response can unlink it */
    if (*buf != NULL) {
        Munmap(*buf, READBATCHSIZE, __func__);
    }

    SemWait(&worker->mutex, __func__);
    SemWait(&worker->full, __func__);

    char *current = worker->area;
    FuncName func = READBATCH;
    memcpy(current, &func, sizeof(func));
    current += sizeof(func);

    uint32 responseId = GetResponseQueueIndex(worker);
    memcpy(current, &responseId, sizeof(responseId));
    current += sizeof(responseId);

    memcpy(current, &relationId, sizeof(relationId));
    current += sizeof(relationId);

    pid_t pid = getpid();
    memcpy(current, &pid, sizeof(pid));
    current += sizeof(pid);

    memcpy(current, &operationId, sizeof(operationId));

    SemPost(&worker->worker, __func__);
    SemPost(&worker->mutex, __func__);

    SemWait(&worker->responseSync[responseId], __func__);

    current = ResponseQueue[responseId];
    memcpy(bufLen, current, sizeof(*bufLen));
    current += sizeof(*bufLen);
    bool hasNext;
    memcpy(&hasNext, current, sizeof(hasNext));

    SemPost(&worker->responseMutex[responseId], __func__);

    if (*bufLen == 0) {
        *buf = NULL;
    } else {
        char filename[FILENAMELENGTH];
        snprintf(filename,
                 FILENAMELENGTH,
                 "%s%d%lu",
                 READBATCHFILE,
                 pid,
                 operationId);
        int fd = ShmOpen(filename, O_RDWR, PERMISSION, __func__);
        *buf = Mmap(NULL,
                    READBATCHSIZE,
                    PROT_READ | PROT_WRITE,
                    MAP_SHARED,
                    fd,
                    0,
                    __func__);
        Fclose(fd, __func__);
    }

    return hasNext;
}

void ReadBatchResponse(char *area) {
//    printf("\n============%s============\n", __func__);

    uint32 *responseId = (uint32 *)area;
    area += sizeof(*responseId);

    KVTableProcOpHashKey iterKey;
    iterKey.relationId = *((Oid *)area);
    area += sizeof(iterKey.relationId);

    iterKey.pid = *((pid_t *)area);
    area += sizeof(iterKey.pid);

    iterKey.operationId = *((uint64 *)area);

    bool found;
    KVHashEntry *entry = hash_search(kvTableHash,
                                     &iterKey.relationId,
                                     HASH_FIND,
                                     &found);
    if (!found) {
        ereport(ERROR, (errmsg("%s failed in hash search", __func__)));
    }

    bool iterFound;
    KVIterHashEntry *iterEntry = hash_search(kvIterHash,
                                             &iterKey,
                                             HASH_ENTER,
                                             &iterFound);
    if (!iterFound) {
        iterEntry->key = iterKey;
        iterEntry->iter = GetIter(entry->db);
    }

    char filename[FILENAMELENGTH];
    snprintf(filename,
             FILENAMELENGTH,
             "%s%d%lu",
             READBATCHFILE,
             iterKey.pid,
             iterKey.operationId);

    ShmUnlink(filename, __func__);
    int fd = ShmOpen(filename,
                     O_CREAT | O_RDWR | O_EXCL,
                     PERMISSION,
                     __func__);
    Ftruncate(fd, READBATCHSIZE, __func__);
    char* buf = Mmap(NULL,
                     READBATCHSIZE,
                     PROT_READ | PROT_WRITE,
                     MAP_SHARED,
                     fd,
                     0,
                     __func__);
    Fclose(fd, __func__);

    size_t bufLen = 0;
    bool hasNext = ReadBatch(entry->db, iterEntry->iter, buf, &bufLen);

    memcpy(ResponseQueue[*responseId], &bufLen, sizeof(bufLen));
    memcpy(ResponseQueue[*responseId] + sizeof(bufLen), &hasNext, sizeof(hasNext));
    Munmap(buf, READBATCHSIZE, __func__);
}

bool GetRequest(Oid relationId,
                WorkerSharedMem *worker,
                char *key,
                size_t keyLen,
                char **val,
                size_t *valLen) {
//    printf("\n============%s============\n", __func__);

    SemWait(&worker->mutex, __func__);
    SemWait(&worker->full, __func__);

    char *current = worker->area;
    FuncName func = GET;
    memcpy(current, &func, sizeof(func));
    current += sizeof(func);

    uint32 responseId = GetResponseQueueIndex(worker);
    memcpy(current, &responseId, sizeof(responseId));
    current += sizeof(responseId);

    memcpy(current, &relationId, sizeof(relationId));
    current += sizeof(relationId);

    memcpy(current, &keyLen, sizeof(keyLen));
    current += sizeof(keyLen);

    memcpy(current, key, keyLen);

    SemPost(&worker->worker, __func__);
    SemPost(&worker->mutex, __func__);
    SemWait(&worker->responseSync[responseId], __func__);

    current = ResponseQueue[responseId];
    bool res;
    memcpy(&res, current, sizeof(res));
    if (!res) {
        SemPost(&worker->responseMutex[responseId], __func__);
        return false;
    }

    current += sizeof(res);
    memcpy(valLen, current, sizeof(*valLen));
    current += sizeof(*valLen);

    *val = palloc(*valLen);
    memcpy(*val, current, *valLen);

    SemPost(&worker->responseMutex[responseId], __func__);

    return true;
}

static void GetResponse(char *area) {
//    printf("\n============%s============\n", __func__);

    int *responseId = (int *)area;
    area += sizeof(*responseId);

    Oid *relationId = (Oid *)area;
    area += sizeof(*relationId);

    bool found;
    KVHashEntry *entry = hash_search(kvTableHash, relationId, HASH_FIND, &found);
    if (!found) {
        ereport(ERROR, (errmsg("%s failed in hash search", __func__)));
    }

    size_t *keyLen = (size_t *)area;
    area += sizeof(*keyLen);

    size_t valLen;
    char *key = area, *val = NULL;
    bool res = Get(entry->db, key, *keyLen, &val, &valLen);
    memcpy(ResponseQueue[*responseId], &res, sizeof(res));
    if (!res) {
        return;
    }

    char *current = ResponseQueue[*responseId] + sizeof(res);
    memcpy(current, &valLen, sizeof(valLen));

    current += sizeof(valLen);
    memcpy(current, val, valLen);

    pfree(val);
}

void PutRequest(Oid relationId,
                WorkerSharedMem *worker,
                char *key,
                size_t keyLen,
                char *val,
                size_t valLen) {
//    printf("\n============%s============\n", __func__);

    SemWait(&worker->mutex, __func__);
    SemWait(&worker->full, __func__);

    char *current = worker->area;
    FuncName func = PUT;
    memcpy(current, &func, sizeof(func));
    current += sizeof(func);

    uint32 responseId = GetResponseQueueIndex(worker);
    memcpy(current, &responseId, sizeof(responseId));
    current += sizeof(responseId);

    memcpy(current, &relationId, sizeof(relationId));
    current += sizeof(relationId);

    memcpy(current, &keyLen, sizeof(keyLen));
    current += sizeof(keyLen);

    memcpy(current, key, keyLen);
    current += keyLen;

    memcpy(current, &valLen, sizeof(valLen));
    current += sizeof(valLen);

    memcpy(current, val, valLen);
    current += valLen;

    if (current - worker->area > BUFSIZE) {
        SemPost(&worker->mutex, __func__);
        SemPost(&worker->responseMutex[responseId], __func__);
        ereport(ERROR,
                (errmsg("%s tuple is too long, increase BUFSIZE", __func__)));
    }

    SemPost(&worker->worker, __func__);
    SemPost(&worker->mutex, __func__);

    SemWait(&worker->responseSync[responseId], __func__);
    SemPost(&worker->responseMutex[responseId], __func__);
}

static void PutResponse(char *area) {
//    printf("\n============%s============\n", __func__);

    Oid *relationId = (Oid *)area;
    area += sizeof(*relationId);

    bool found;
    KVHashEntry *entry = hash_search(kvTableHash, relationId, HASH_FIND, &found);
    if (!found) {
        ereport(ERROR, (errmsg("%s failed in hash search", __func__)));
    }

    size_t *keyLen = (size_t *)area;
    area += sizeof(*keyLen);

    char *key = area;
    area += *keyLen;

    size_t *valLen = (size_t *)area;
    area += sizeof(*valLen);

    char *val = area;
    if (!Put(entry->db, key, *keyLen, val, *valLen)) {
        ereport(ERROR, (errmsg("error from %s", __func__)));
    }
}

void DeleteRequest(Oid relationId,
                   WorkerSharedMem *worker,
                   char *key,
                   size_t keyLen) {
//    printf("\n============%s============\n", __func__);

    SemWait(&worker->mutex, __func__);
    SemWait(&worker->full, __func__);

    char *current = worker->area;
    FuncName func = DELETE;
    memcpy(current, &func, sizeof(func));
    current += sizeof(func);

    uint32 responseId = GetResponseQueueIndex(worker);
    memcpy(current, &responseId, sizeof(responseId));
    current += sizeof(responseId);

    memcpy(current, &relationId, sizeof(relationId));
    current += sizeof(relationId);

    memcpy(current, &keyLen, sizeof(keyLen));
    current += sizeof(keyLen);

    memcpy(current, key, keyLen);

    SemPost(&worker->worker, __func__);
    SemPost(&worker->mutex, __func__);

    SemWait(&worker->responseSync[responseId], __func__);
    SemPost(&worker->responseMutex[responseId], __func__);
}

static void DeleteResponse(char *area) {
//    printf("\n============%s============\n", __func__);

    Oid *relationId = (Oid *)area;
    area += sizeof(*relationId);

    bool found;
    KVHashEntry *entry = hash_search(kvTableHash, relationId, HASH_FIND, &found);
    if (!found) {
        ereport(ERROR, (errmsg("%s failed in hash search", __func__)));
    }

    size_t *keyLen = (size_t *)area;
    area += sizeof(*keyLen);

    char *key = area;
    if (!Delete(entry->db, key, *keyLen)) {
        ereport(ERROR, (errmsg("error from %s", __func__)));
    }
}

RingBufferSharedMem* BeginLoadRequest(Oid relationId,
                                      WorkerSharedMem *worker) {
    // printf("\n============%s============\n", __func__);

    pid_t pid = getpid();

    /* mmap ring buffer shared memory */
    char filename[FILENAMELENGTH];
    snprintf(filename,
             FILENAMELENGTH,
             "%s%d%d",
             LOADFILE,
             pid,
             relationId);

    ShmUnlink(filename, __func__);
    int fd = ShmOpen(filename,
                     O_CREAT | O_RDWR | O_EXCL,
                     PERMISSION,
                     __func__);
    Ftruncate(fd, sizeof(RingBufferSharedMem), __func__);
    RingBufferSharedMem* buf = Mmap(NULL,
                                    sizeof(RingBufferSharedMem),
                                    PROT_READ | PROT_WRITE,
                                    MAP_SHARED,
                                    fd,
                                    0,
                                    __func__);
    Fclose(fd, __func__);

    /* init ring buffer shared memory */
    buf->finish = false;
    buf->in = buf->out = buf->count = 0;
    SemInit(&buf->mutext, 1, 1, __func__);
    SemInit(&buf->empty, 1, 0, __func__);
    SemInit(&buf->full, 1, 0, __func__);
    SemInit(&buf->worker, 1, 0, __func__);

    /* send load request */
    SemWait(&worker->mutex, __func__);
    SemWait(&worker->full, __func__);

    char *current = worker->area;
    FuncName func = LOAD;
    memcpy(current, &func, sizeof(func));
    current += sizeof(func);

    uint32 responseId = GetResponseQueueIndex(worker);
    memcpy(current, &responseId, sizeof(responseId));
    current += sizeof(responseId);

    memcpy(current, &relationId, sizeof(relationId));
    current += sizeof(relationId);

    memcpy(current, &pid, sizeof(pid));

    SemPost(&worker->worker, __func__);
    SemPost(&worker->mutex, __func__);

    SemWait(&worker->responseSync[responseId], __func__);
    SemPost(&worker->responseMutex[responseId], __func__);

    return buf;
}

static void WriteRingBuffer(RingBufferSharedMem* buf, uint64* offset,
                            const char* data, size_t size) {
    char *input = buf->area + *offset;
    if (*offset + size > LOADBUFFSIZE) {
        /* circular write */
        size_t n = LOADBUFFSIZE - *offset;
        memcpy(input, data, n);
        input = buf->area;
        *offset = 0;

        memcpy(input, data + n, size - n);
        *offset = *offset + (size - n);
    } else {
        memcpy(input, data, size);
        *offset = *offset + size;
        if (*offset == LOADBUFFSIZE) {
            *offset = 0;
        }
    }
}

void LoadTuple(RingBufferSharedMem* buf,
               char *key,
               size_t keyLen,
               char *val,
               size_t valLen) {
    // printf("\n============%s============\n", __func__);

    /* total_size + key_size + key + value */
    size_t tupleLen = sizeof(size_t) + sizeof(size_t) +
                      keyLen + valLen;

    /* check whether the buffer is enough */
    while (true) {
        uint64 empty_slot = 0;

        SemWait(&buf->mutext, __func__);
        if (buf->out > buf->in) {
            empty_slot = buf->out - buf->in;
        } else {
            empty_slot = LOADBUFFSIZE - buf->in + buf->out;
        }
        SemPost(&buf->mutext, __func__);

        /*
         * reserve an empty slot to avoid that
         * the in offset is equal to the out
         * offset when the buffer is full, namely,
         * the in offset will never catch up with
         * the out offset.
         */
        if (empty_slot < tupleLen + 1) {
            SemWait(&buf->full, __func__);
            /*
             * maybe the empty slot is still unenough
             * even if has read some small tuples.
             */
        } else {  /* enough */
            break;
        }
    }

    /* write tuple into ring buffer */
    uint64 offset = buf->in;  /* current write offset */
    WriteRingBuffer(buf, &offset, (const char *) &tupleLen, sizeof(size_t));
    WriteRingBuffer(buf, &offset, (const char *) &keyLen, sizeof(size_t));
    WriteRingBuffer(buf, &offset, key, keyLen);
    WriteRingBuffer(buf, &offset, val, valLen);

    /* reset the in offset */
    SemWait(&buf->mutext, __func__);
    buf->in = offset;
    SemPost(&buf->mutext, __func__);
    SemPost(&buf->empty, __func__);
}

static void ReadRingBuffer(RingBufferSharedMem* buf, uint64* offset,
                           char** data, size_t size) {
    char *output = buf->area + *offset;
    size_t n = LOADBUFFSIZE - *offset;

    if (n < size) {  /* circular read */
        memcpy(*data, output, n);
        memset(output, 0, n);
        output = buf->area;
        *data = *data + n;
        *offset = 0;

        memcpy(*data, output, size - n);
        memset(output, 0, size - n);
        *data = *data + (size - n);
        *offset = *offset + (size - n);
    } else {
        memcpy(*data, output, size);
        memset(output, 0, size);
        *data = *data + size;
        *offset = *offset + size;
        if (*offset == LOADBUFFSIZE) {
            *offset = 0;
        }
    }
}

static void LoadResponse(char *area) {
    // printf("\n============%s============\n", __func__);

    Oid *relationId = (Oid *)area;
    area += sizeof(*relationId);

    pid_t *pid = (pid_t*)area;
    area += sizeof(*pid);

    bool found;
    KVHashEntry *entry = hash_search(kvTableHash, relationId, HASH_FIND, &found);
    if (!found) {
        ereport(ERROR, (errmsg("%s failed in hash search", __func__)));
    }

    /* mmap ring buffer shared memory */
    char filename[FILENAMELENGTH];
    snprintf(filename,
             FILENAMELENGTH,
             "%s%d%d",
             LOADFILE,
             *pid,
             *relationId);
    int fd = ShmOpen(filename, O_RDWR, PERMISSION, __func__);
    RingBufferSharedMem* buf = Mmap(NULL,
                                    sizeof(RingBufferSharedMem),
                                    PROT_READ | PROT_WRITE,
                                    MAP_SHARED,
                                    fd,
                                    0,
                                    __func__);
    Fclose(fd, __func__);

    char tuple[BUFSIZE];
    bool finish = false;
    buf->count = 0;

    while (true) {
        /*
         * the buffer is empty when the out offset
         * catches up with the in offset, namely,
         * the out offset is equal to the in offset.
         */
        while (true) {
            uint64 delta = 0;

            SemWait(&buf->mutext, __func__);
            delta = buf->in - buf->out;
            SemPost(&buf->mutext, __func__);

            if (delta == 0) {  /* empty */
                if (buf->finish) {
                    finish = true;
                    break;  /* has read all data */
                } else {
                    SemWait(&buf->empty, __func__);
                }
            } else {  /* at least one tuple */
                break;
            }
        }

        if (finish) {
            break;
        }

        memset(tuple, 0, BUFSIZ);  /* reset tuple buffer */
        char *tupleptr = tuple;    /* tuple write ptr */
        uint64 offset = buf->out;  /* current read offset */

        /* extract the tuple's total size */
        ReadRingBuffer(buf, &offset, &tupleptr, sizeof(size_t));
        /* extract the tuple's kv data */
        size_t* tupleLen = (size_t*) tuple;
        size_t dataLen = *tupleLen - sizeof(size_t);
        ReadRingBuffer(buf, &offset, &tupleptr, dataLen);

        /* reset the out offset */
        SemWait(&buf->mutext, __func__);
        buf->out = offset;
        SemPost(&buf->mutext, __func__);
        SemPost(&buf->full, __func__);

        /* put tuple into storage engine */
        char *current = tuple + sizeof(size_t);
        size_t *keyLen = (size_t *) current;
        current = current + sizeof(*keyLen);
        char *key = (char *) current;
        current = current + *keyLen;
        char *val = (char *) current;
        /* total_size + key_size + key + value */
        size_t valLen = *tupleLen - 2*sizeof(size_t) - *keyLen;
        if (!Put(entry->db, key, *keyLen, val, valLen)) {
            ereport(ERROR, (errmsg("error from %s", __func__)));
        }

        buf->count = buf->count + 1;
    }

    // printf("\nLOAD %ld rows\n", buf->count);
    SemPost(&buf->worker, __func__);

    /* clean temporary resource */
    Munmap(buf, sizeof(*buf), __func__);
    ShmUnlink(filename, __func__);
}

uint64 EndLoadRequest(Oid relationId,
                      WorkerSharedMem *worker,
                      RingBufferSharedMem* buf) {
    // printf("\n============%s============\n", __func__);

    /* mark the finish flag */
    buf->finish = true;
    SemPost(&buf->empty, __func__);
    SemWait(&buf->worker, __func__);

    uint64 count = buf->count;

    /* clean temporary resource */
    SemDestroy(&buf->full, __func__);
    SemDestroy(&buf->empty, __func__);
    SemDestroy(&buf->mutext, __func__);
    SemDestroy(&buf->worker, __func__);

    pid_t pid = getpid();
    char filename[FILENAMELENGTH];
    snprintf(filename,
             FILENAMELENGTH,
             "%s%d%d",
             LOADFILE,
             pid,
             relationId);
    Munmap(buf, sizeof(*buf), __func__);
    ShmUnlink(filename, __func__);

    return count;
}

#ifdef VIDARDB
/*
 * The communication model for range query is different from other queries.
 * shared mem will be created and opened multiple times even in the user level
 * of the same range query. unmap must be issued in both sides, not once in a
 * life anymore.
 * options != NULL means first time trigger this function.
 * Return whether there is a remaining batch.
 */
bool RangeQueryRequest(Oid relationId,
                       uint64 operationId,
                       WorkerSharedMem *worker,
                       RangeQueryOptions *options,
                       char **buf,
                       size_t *bufLen) {
//    printf("\n============%s============\n", __func__);

    /* munmap the shared memory so that Response can unlink it */
    if (*buf && *bufLen > 0) {
        Munmap(*buf, *bufLen, __func__);
    }

    SemWait(&worker->mutex, __func__);
    SemWait(&worker->full, __func__);

    char *current = worker->area;
    FuncName func = RANGEQUERY;
    memcpy(current, &func, sizeof(func));
    current += sizeof(func);

    uint32 responseId = GetResponseQueueIndex(worker);
    memcpy(current, &responseId, sizeof(responseId));
    current += sizeof(responseId);

    memcpy(current, &relationId, sizeof(relationId));
    current += sizeof(relationId);

    pid_t pid = getpid();
    memcpy(current, &pid, sizeof(pid));
    current += sizeof(pid);

    memcpy(current, &operationId, sizeof(operationId));
    current += sizeof(operationId);

    /* options != NULL means first time trigger this function */
    if (options) {
        memcpy(current, &(options->startLen), sizeof(options->startLen));
        current += sizeof(options->startLen);
        if (options->startLen > 0) {
            memcpy(current, options->start, options->startLen);
            current += options->startLen;
        }

        memcpy(current, &(options->limitLen), sizeof(options->limitLen));
        current += sizeof(options->limitLen);
        if (options->limitLen > 0) {
            memcpy(current, options->limit, options->limitLen);
            current += options->limitLen;
        }

        memcpy(current, &(options->batchCapacity), sizeof(options->batchCapacity));
        current += sizeof(options->batchCapacity);

        memcpy(current, &(options->attrCount), sizeof(options->attrCount));
        current += sizeof(options->attrCount);
        if (options->attrCount > 0) {
            memcpy(current,
                   options->attrs,
                   options->attrCount * sizeof(*(options->attrs)));
        }
    }

    SemPost(&worker->worker, __func__);
    SemPost(&worker->mutex, __func__);
    SemWait(&worker->responseSync[responseId], __func__);

    current = ResponseQueue[responseId];
    memcpy(bufLen, current, sizeof(*bufLen));
    current += sizeof(*bufLen);

    bool hasNext;
    memcpy(&hasNext, current, sizeof(hasNext));

    if (*bufLen == 0) {
        *buf = NULL;
    } else {
        char filename[FILENAMELENGTH];
        snprintf(filename,
                 FILENAMELENGTH,
                 "%s%d%lu",
                 RANGEQUERYFILE,
                 pid,
                 operationId);
        int fd = ShmOpen(filename, O_RDWR, PERMISSION, __func__);
        *buf = Mmap(NULL,
                    *bufLen,  /* must larger than 0 */
                    PROT_READ | PROT_WRITE,
                    MAP_SHARED,
                    fd,
                    0,
                    __func__);
        Fclose(fd, __func__);
    }

    SemPost(&worker->responseMutex[responseId], __func__);
    return hasNext;
}

static void RangeQueryResponse(char *area) {
//    printf("\n============%s============\n", __func__);

    uint32 *responseId = (uint32 *)area;
    area += sizeof(*responseId);

    KVTableProcOpHashKey optionKey;
    optionKey.relationId = *((Oid *)area);
    area += sizeof(optionKey.relationId);

    optionKey.pid = *((pid_t *)area);
    area += sizeof(optionKey.pid);

    optionKey.operationId = *((uint64 *)area);
    area += sizeof(optionKey.operationId);

    bool found = false;
    KVHashEntry *entry = hash_search(kvTableHash,
                                     &optionKey.relationId,
                                     HASH_FIND,
                                     &found);
    if (!found) {
        ereport(ERROR, (errmsg("%s failed in hash search", __func__)));
    }

    bool optionFound = false;
    KVReadOptionsEntry *optionEntry = hash_search(kvReadOptionsHash,
                                                  &optionKey,
                                                  HASH_ENTER,
                                                  &optionFound);
    if (!optionFound) {
        /*
         * first time to trigger this func for the range query
         * so pass rangequeryOptions, and build range and readOptions
         */
        optionEntry->key = optionKey;
        optionEntry->readOptions = NULL;
        optionEntry->range = NULL;

        RangeQueryOptions options;

        options.startLen = *((size_t *)area);
        area += sizeof(options.startLen);
        if (options.startLen > 0) {
            options.start = palloc(options.startLen);
            memcpy(options.start, area, options.startLen);
            area += options.startLen;
        }

        options.limitLen = *((size_t *)area);
        area += sizeof(options.limitLen);
        if (options.limitLen > 0) {
            options.limit = palloc(options.limitLen);
            memcpy(options.limit, area, options.limitLen);
            area += options.limitLen;
        }

        options.batchCapacity = *((size_t *)area);
        area += sizeof(options.batchCapacity);

        options.attrCount = *((int *)area);
        area += sizeof(options.attrCount);
        if (options.attrCount > 0) {
            options.attrs = (AttrNumber *)area;
        }

        ParseRangeQueryOptions(&options,
                               &(optionEntry->range),
                               &(optionEntry->readOptions));

    }

    void *result = NULL;
    size_t bufLen = 0;
    bool ret = false;
    do {
        ret = RangeQuery(entry->db,
                         optionEntry->range,
                         &(optionEntry->readOptions),
                         &bufLen,
                         &result);
    } while (ret && bufLen == 0);

    char *current = ResponseQueue[*responseId];
    memcpy(current, &bufLen, sizeof(bufLen));
    current += sizeof(bufLen);
    memcpy(current, &ret, sizeof(ret));

    char filename[FILENAMELENGTH];
    snprintf(filename,
             FILENAMELENGTH,
             "%s%d%lu",
             RANGEQUERYFILE,
             optionKey.pid,
             optionKey.operationId);
    /*
     * clear last call's shared data structure,
     * might throw out warning if no object to unlink, but it is fine.
     */
    ShmUnlink(filename, __func__);

    char *buf = NULL;
    if (bufLen > 0) {
        int fd = ShmOpen(filename,
                         O_CREAT | O_RDWR | O_EXCL,
                         PERMISSION,
                         __func__);
        Ftruncate(fd, bufLen, __func__);
        buf = Mmap(NULL,
                   bufLen,  /* must larger than 0 */
                   PROT_READ | PROT_WRITE,
                   MAP_SHARED,
                   fd,
                   0,
                   __func__);
        Fclose(fd, __func__);
    }

    /* even bufLen==0, call it to delete result */
    ParseRangeQueryResult(result, buf);

    /*
     * It is safe to unmap before request side reading
     * as long as shm_unlink is not issued.
     */
    if (bufLen > 0) {
        Munmap(buf, bufLen, __func__);
    }
}

void ClearRangeQueryMetaRequest(Oid relationId,
                                uint64 operationId,
                                WorkerSharedMem *worker,
                                TableReadState *readState) {
//    printf("\n============%s============\n", __func__);

    if (readState->buf && readState->bufLen > 0) {
        Munmap(readState->buf, readState->bufLen, __func__);
    }

    SemWait(&worker->mutex, __func__);
    SemWait(&worker->full, __func__);

    char *current = worker->area;
    FuncName func = CLEARRQMETA;
    memcpy(current, &func, sizeof(func));
    current += sizeof(func);

    uint32 responseId = GetResponseQueueIndex(worker);
    memcpy(current, &responseId, sizeof(responseId));
    current += sizeof(responseId);

    memcpy(current, &relationId, sizeof(relationId));
    current += sizeof(relationId);

    pid_t pid = getpid();
    memcpy(current, &pid, sizeof(pid));
    current += sizeof(pid);

    memcpy(current, &operationId, sizeof(operationId));

    SemPost(&worker->worker, __func__);
    SemPost(&worker->mutex, __func__);

    SemWait(&worker->responseSync[responseId], __func__);
    SemPost(&worker->responseMutex[responseId], __func__);
}

static void ClearRangeQueryMetaResponse(char *area) {
//    printf("\n============%s============\n", __func__);

    KVTableProcOpHashKey optionKey;
    optionKey.relationId = *((Oid *)area);
    area += sizeof(optionKey.relationId);

    optionKey.pid = *((pid_t *)area);
    area += sizeof(optionKey.pid);

    optionKey.operationId = *((uint64 *)area);

    bool found;
    KVReadOptionsEntry *entry = hash_search(kvReadOptionsHash,
                                            &optionKey,
                                            HASH_REMOVE,
                                            &found);
    if (!found) {
        ereport(ERROR, (errmsg("%s failed in hash search", __func__)));
    }

    ClearRangeQueryMeta(entry->range, entry->readOptions);
    /* might reuse, so must set NULL */
    entry->readOptions = NULL;
    entry->range = NULL;

    char filename[FILENAMELENGTH];
    snprintf(filename,
             FILENAMELENGTH,
             "%s%d%lu",
             RANGEQUERYFILE,
             optionKey.pid,
             optionKey.operationId);
    ShmUnlink(filename, __func__);
}
#endif
