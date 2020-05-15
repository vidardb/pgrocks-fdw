
#include "kv_fdw.h"
#include "kv_storage.h"
#include "kv_posix.h"

#include <fcntl.h>

#include "postmaster/fork_process.h"
#include "postmaster/postmaster.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "utils/ps_status.h"
#include "utils/hsearch.h"


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

HTAB *kvReadOptionsHash = NULL;
#endif


pid_t kvWorkerPid = 0;  // in postmaster process

HTAB *kvTableHash = NULL;  // in kvworker process

HTAB *kvIterHash = NULL;  // in kvworker process

long HASHSIZE = 1;  // non-shared hash can be enlarged

/*
 * referenced by thread of postmaster process, client process, worker process
 */
char *ResponseQueue[RESPONSEQUEUELENGTH];


static int StartKVWorker(void);

static void OpenResponse(char *area);

static void CloseResponse(char *area);

static void CountResponse(char *area);

static void GetIterResponse(char *area);

static void DelIterResponse(char *area);

static void NextResponse(char *area);

static void ReadBatchResponse(char *area);

static void GetResponse(char *area);

static void PutResponse(char *area);

static void DeleteResponse(char *area);

#ifdef VIDARDB
static void RangeQueryResponse(char *area);

static void ClearRangeQueryMetaResponse(char *area);
#endif


/*
 * A child process must acquire the mutex of the shared memory before calling
 * this functions, so processes check the available response slot in the FIFO
 * manner. If all the response slots are used by other processes, the caller
 * process will loop here. Called by the thread in postmaster process and client
 * process.
 */
static inline uint32 GetResponseQueueIndex(SharedMem *ptr) {
    while (true) {
        for (uint32 i = 0; i < RESPONSEQUEUELENGTH; i++) {
            int ret = SemTryWait(&ptr->responseMutex[i], __func__);
            if (ret == 0) {
                return i;
            }
        }
    }
}

/*
 * called by the thread in postmaster process
 */
static void cleanup_handler(void *arg) {
    printf("\n============%s============\n", __func__);
    SharedMem *ptr = *((SharedMem **)arg);

    if (kvWorkerPid != 0) {
        FuncName func = TERMINATE;
        SemWait(&ptr->mutex, __func__);
        SemWait(&ptr->full, __func__);
        memcpy(ptr->area, &func, sizeof(func));
        uint32 responseId = GetResponseQueueIndex(ptr);
        memcpy(ptr->area + sizeof(func), &responseId, sizeof(responseId));
        SemPost(&ptr->worker, __func__);
        SemWait(&ptr->responseSync[responseId], __func__);
        kvWorkerPid = 0;
    }

    // release the response area first
    for (uint32 i = 0; i < RESPONSEQUEUELENGTH; i++) {
        Munmap(ResponseQueue[i], BUFSIZE, __func__);
        char filename[FILENAMELENGTH];
        snprintf(filename, FILENAMELENGTH, "%s%d", RESPONSEFILE, i);
        ShmUnlink(filename, __func__);
    }
    
    SemDestroy(&ptr->mutex, __func__);
    SemDestroy(&ptr->full, __func__);
    SemDestroy(&ptr->agent[0], __func__);
    SemDestroy(&ptr->agent[1], __func__);
    SemDestroy(&ptr->worker, __func__);

    for (uint32 i = 0; i < RESPONSEQUEUELENGTH; i++) {
        SemDestroy(&ptr->responseMutex[i], __func__);
        SemDestroy(&ptr->responseSync[i], __func__);
    }

    Munmap(ptr, sizeof(*ptr), __func__);
    ShmUnlink(BACKFILE, __func__);
}

/*
 * Initialize shared memory for responses
 * called by the thread in postmaster process
 */
static void InitResponseArea() {
    for (uint32 i = 0; i < RESPONSEQUEUELENGTH; i++) {
        char filename[FILENAMELENGTH];
        snprintf(filename, FILENAMELENGTH, "%s%d", RESPONSEFILE, i);
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
 * called by worker and client process
 */
static void OpenResponseArea() {
    for (uint32 i = 0; i < RESPONSEQUEUELENGTH; i++) {
        if (ResponseQueue[i] == NULL) {
            char filename[FILENAMELENGTH];
            snprintf(filename, FILENAMELENGTH, "%s%d", RESPONSEFILE, i);
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

void *KVStorageThreadFun(void *arg) {
    PthreadSetCancelState(PTHREAD_CANCEL_ENABLE, NULL, __func__);
    PthreadSetCancelType(PTHREAD_CANCEL_DEFERRED, NULL, __func__);

    SharedMem *ptr = NULL;
    pthread_cleanup_push(cleanup_handler, &ptr);

    ShmUnlink(BACKFILE, __func__);
    int fd = ShmOpen(BACKFILE, O_CREAT | O_RDWR | O_EXCL, PERMISSION, __func__);
    Ftruncate(fd, sizeof(*ptr), __func__);
    ptr = Mmap(NULL,
               sizeof(*ptr),
               PROT_READ | PROT_WRITE,
               MAP_SHARED,
               fd,
               0,
               __func__);
    Fclose(fd, __func__);

    // Initialize the response area
    InitResponseArea();

    SemInit(&ptr->mutex, 1, 1, __func__);
    SemInit(&ptr->full, 1, 1, __func__);
    SemInit(&ptr->agent[0], 1, 0, __func__);
    SemInit(&ptr->agent[1], 1, 0, __func__);
    SemInit(&ptr->worker, 1, 0, __func__);

    for (uint32 i = 0; i < RESPONSEQUEUELENGTH; i++) {
        SemInit(&ptr->responseMutex[i], 1, 1, __func__);
        SemInit(&ptr->responseSync[i], 1, 0, __func__);
    }

    ptr->workerProcessCreated = false;

    do {
        // don't create worker process until needed!
        SemWait(&ptr->agent[0], __func__);

        kvWorkerPid = StartKVWorker();
        ptr->workerProcessCreated = true;

        SemPost(&ptr->agent[1], __func__);
    } while (true);

    pthread_cleanup_pop(1);
    return NULL;
}

/*
 * Main loop for the KVWorker process.
 */
static void KVWorkerMain(int argc, char *argv[]) {
    init_ps_display("kvworker", "", "", "");

    ereport(DEBUG1, (errmsg("kvworker started")));

    int fd = ShmOpen(BACKFILE, O_RDWR, PERMISSION, __func__);
    SharedMem *ptr = Mmap(NULL,
                          sizeof(*ptr),
                          PROT_READ | PROT_WRITE,
                          MAP_SHARED,
                          fd,
                          0,
                          __func__);
    Fclose(fd, __func__);

    // open the response queue
    OpenResponseArea();

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
        SemWait(&ptr->worker, __func__);

        FuncName func;
        memcpy(&func, ptr->area, sizeof(func));
        uint32 responseId;
        memcpy(&responseId, ptr->area + sizeof(func), sizeof(responseId));

        memset(buf, 0, BUFSIZE);
        memcpy(buf, ptr->area + sizeof(func), BUFSIZE - sizeof(func));
        SemPost(&ptr->full, __func__);

        if (func == TERMINATE) {
            SemPost(&ptr->responseSync[responseId], __func__);
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
            case NEXT:
                NextResponse(buf);
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
            default:
                ereport(ERROR, (errmsg("%s failed in switch", __func__)));
        }

        SemPost(&ptr->responseSync[responseId], __func__);
    } while (true);

    HASH_SEQ_STATUS status;
    hash_seq_init(&status, kvTableHash);
    KVHashEntry *entry = NULL;
    while ((entry = hash_seq_search(&status)) != NULL) {
        printf("\n ref count %d\n", entry->ref);
        Close(entry->db);
    }

    ereport(DEBUG1, (errmsg("kvworker shutting down")));

    proc_exit(0);               /* done */
}

static int StartKVWorker(void) {
    pid_t kvWorkerPid;

    switch (kvWorkerPid = fork_process()) {
        case -1:
            ereport(ERROR, (errmsg("could not fork kvworker process")));
            return 0;
        case 0:
            /* in postmaster child ... */
            InitPostmasterChild();

            /* Close the postmaster's sockets */
            ClosePostmasterPorts(false);

            KVWorkerMain(0, NULL);
            break;
        default:
            return (int) kvWorkerPid;
    }

    /* shouldn't get here */
    return 0;
}

SharedMem *OpenRequest(Oid relationId, SharedMem *ptr, ...) {
//    printf("\n============%s============\n", __func__);

    if (!ptr) {
        /*
         * Client process connects to the shared mem created by server.
         * We don't do unmap in the client side, because the of cost of
         * mapped area is just one-time for the whole life of client.
         */
        int fd = ShmOpen(BACKFILE, O_RDWR, PERMISSION, __func__);
        ptr = Mmap(NULL,
                   sizeof(*ptr),
                   PROT_READ | PROT_WRITE,
                   MAP_SHARED,
                   fd,
                   0,
                   __func__);
        Fclose(fd, __func__);

        OpenResponseArea();
    }

    // lock among child processes
    SemWait(&ptr->mutex, __func__);

    if (!ptr->workerProcessCreated) {
        SemPost(&ptr->agent[0], __func__);
        SemWait(&ptr->agent[1], __func__);
    }

    // wait for the worker process copy out the previous request
    SemWait(&ptr->full, __func__);

    // open request does not need a response
    char *current = ptr->area;
    FuncName func = OPEN;
    memcpy(current, &func, sizeof(func));
    current += sizeof(func);

    uint32 responseId = GetResponseQueueIndex(ptr);
    memcpy(current, &responseId, sizeof(responseId));
    current += sizeof(responseId);

    #ifdef VIDARDB
    va_list vl;
    va_start(vl, ptr);

    bool useColumn = (bool) va_arg(vl, int);
    memcpy(current, &useColumn, sizeof(useColumn));
    current += sizeof(useColumn);

    int attrCount = va_arg(vl, int);
    memcpy(current, &attrCount, sizeof(attrCount));
    current += sizeof(attrCount);
    va_end(vl);
    #endif

    KVFdwOptions *fdwOptions = KVGetOptions(relationId);
    char *path = fdwOptions->filename;
    strcpy(current, path);

    SemPost(&ptr->worker, __func__);
    // unlock
    SemPost(&ptr->mutex, __func__);

    SemWait(&ptr->responseSync[responseId], __func__);
    SemPost(&ptr->responseMutex[responseId], __func__);
    return ptr;
}

static void OpenResponse(char *area) {
//    printf("\n============%s============\n", __func__);

    #ifdef VIDARDB
    bool *useColumn = (bool *)area;
    area += sizeof(*useColumn);

    int *attrCount = (int *)area;
    area += sizeof(*attrCount);
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
        entry->db = Open(path, *useColumn, *attrCount);
        #else
        entry->db = Open(path);
        #endif
    } else {
        entry->ref++;
//        printf("\n%s ref %d\n", __func__, entry->ref);
    }
}

void CloseRequest(Oid relationId, SharedMem *ptr) {
//    printf("\n============%s============\n", __func__);

    SemWait(&ptr->mutex, __func__);
    SemWait(&ptr->full, __func__);

    char *current = ptr->area;
    FuncName func = CLOSE;
    memcpy(current, &func, sizeof(func));
    current += sizeof(func);

    uint32 responseId = GetResponseQueueIndex(ptr);
    memcpy(current, &responseId, sizeof(responseId));
    current += sizeof(responseId);

    memcpy(current, &relationId, sizeof(relationId));

    SemPost(&ptr->worker, __func__);
    SemPost(&ptr->mutex, __func__);

    SemWait(&ptr->responseSync[responseId], __func__);
    SemPost(&ptr->responseMutex[responseId], __func__);
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

uint64 CountRequest(Oid relationId, SharedMem *ptr) {
//    printf("\n============%s============\n", __func__);

    SemWait(&ptr->mutex, __func__);
    SemWait(&ptr->full, __func__);

    char *current = ptr->area;
    FuncName func = COUNT;
    memcpy(current, &func, sizeof(func));
    current += sizeof(func);

    uint32 responseId = GetResponseQueueIndex(ptr);
    memcpy(current, &responseId, sizeof(responseId));
    current += sizeof(responseId);

    memcpy(current, &relationId, sizeof(relationId));

    SemPost(&ptr->worker, __func__);
    SemPost(&ptr->mutex, __func__);

    SemWait(&ptr->responseSync[responseId], __func__);
    uint64 count;
    memcpy(&count, ResponseQueue[responseId], sizeof(count));
    SemPost(&ptr->responseMutex[responseId], __func__);
    return count;
}

static void CountResponse(char *area) {
//    printf("\n============%s============\n", __func__);

    uint32 *responseId = (uint32 *)area;
    area += sizeof(*responseId);

    Oid *relationId = (Oid *)area;

    bool found;
    KVHashEntry *entry = hash_search(kvTableHash, &relationId, HASH_FIND, &found);
    if (!found) {
        ereport(ERROR, (errmsg("%s failed in hash search", __func__)));
    }

    uint64 count = Count(entry->db);
    memcpy(ResponseQueue[*responseId], &count, sizeof(count));
}

void GetIterRequest(Oid relationId, uint64 operationId, SharedMem *ptr) {
//    printf("\n============%s============\n", __func__);

    SemWait(&ptr->mutex, __func__);
    SemWait(&ptr->full, __func__);

    char *current = ptr->area;
    FuncName func = GETITER;
    memcpy(current, &func, sizeof(func)); 
    current += sizeof(func);

    uint32 responseId = GetResponseQueueIndex(ptr);
    memcpy(current, &responseId, sizeof(responseId));
    current += sizeof(responseId);

    memcpy(current, &relationId, sizeof(relationId));
    current += sizeof(relationId);

    pid_t pid = getpid();
    memcpy(current, &pid, sizeof(pid));
    current += sizeof(pid);

    memcpy(current, &operationId, sizeof(operationId));

    SemPost(&ptr->worker, __func__);
    SemPost(&ptr->mutex, __func__);

    SemWait(&ptr->responseSync[responseId], __func__);
    SemPost(&ptr->responseMutex[responseId], __func__);
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
                    SharedMem *ptr,
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

    SemWait(&ptr->mutex, __func__);
    SemWait(&ptr->full, __func__);

    char *current = ptr->area;
    FuncName func = DELITER;
    memcpy(current, &func, sizeof(func));
    current += sizeof(func);

    uint32 responseId = GetResponseQueueIndex(ptr);
    memcpy(current, &responseId, sizeof(responseId));
    current += sizeof(responseId);

    memcpy(current, &relationId, sizeof(relationId));
    current += sizeof(relationId);

    memcpy(current, &pid, sizeof(pid));
    current += sizeof(pid);

    memcpy(current, &operationId, sizeof(operationId));

    SemPost(&ptr->worker, __func__);
    SemPost(&ptr->mutex, __func__);

    SemWait(&ptr->responseSync[responseId], __func__);
    SemPost(&ptr->responseMutex[responseId], __func__);
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

bool NextRequest(Oid relationId,
                 uint64 operationId,
                 SharedMem *ptr,
                 char **key,
                 size_t *keyLen,
                 char **val,
                 size_t *valLen) {
//    printf("\n============%s============\n", __func__);

    SemWait(&ptr->mutex, __func__);
    SemWait(&ptr->full, __func__);

    char *current = ptr->area;
    FuncName func = NEXT;
    memcpy(current, &func, sizeof(func));
    current += sizeof(func);

    uint32 responseId = GetResponseQueueIndex(ptr);
    memcpy(current, &responseId, sizeof(responseId));
    current += sizeof(responseId);

    memcpy(current, &relationId, sizeof(relationId));
    current += sizeof(relationId);

    pid_t pid = getpid();
    memcpy(current, &pid, sizeof(pid));
    current += sizeof(pid);

    memcpy(current, &operationId, sizeof(operationId));


    SemPost(&ptr->worker, __func__);
    SemPost(&ptr->mutex, __func__);

    SemWait(&ptr->responseSync[responseId], __func__);

    current = ResponseQueue[responseId];
    memcpy(keyLen, current, sizeof(*keyLen));

    /* no next item */
    if (*keyLen == 0) {
        SemPost(&ptr->responseMutex[responseId], __func__);
        return false;
    }

    current += sizeof(*keyLen);
    *key = palloc(*keyLen);
    memcpy(*key, current, *keyLen);

    current += *keyLen;
    memcpy(valLen, current, sizeof(*valLen));

    current += sizeof(*valLen);
    *val = palloc(*valLen);
    memcpy(*val, current, *valLen);

    SemPost(&ptr->responseMutex[responseId], __func__);
    return true;
}

void NextResponse(char *area) {
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
                                             HASH_FIND,
                                             &iterFound);
    if (!iterFound) {
        ereport(ERROR, (errmsg("%s failed in hash search for iterator", __func__)));
    }

    char *current = ResponseQueue[*responseId];
    bool res = Next(entry->db, iterEntry->iter, current);
    if (!res) {
        /* no next item */
        size_t keyLen = 0;
        memcpy(ResponseQueue[*responseId], &keyLen, sizeof(keyLen));
    }
}

bool ReadBatchRequest(Oid relationId,
                      uint64 operationId,
                      SharedMem *ptr,
                      char **buf,
                      size_t *bufLen) {
//    printf("\n============%s============\n", __func__);

    /* munmap the shared memory so that Response can unlink it */
    if (*buf != NULL) {
        Munmap(*buf, READBATCHSIZE, __func__);
    }

    SemWait(&ptr->mutex, __func__);
    SemWait(&ptr->full, __func__);

    char *current = ptr->area;
    FuncName func = READBATCH;
    memcpy(current, &func, sizeof(func));
    current += sizeof(func);

    uint32 responseId = GetResponseQueueIndex(ptr);
    memcpy(current, &responseId, sizeof(responseId));
    current += sizeof(responseId);

    memcpy(current, &relationId, sizeof(relationId));
    current += sizeof(relationId);

    pid_t pid = getpid();
    memcpy(current, &pid, sizeof(pid));
    current += sizeof(pid);

    memcpy(current, &operationId, sizeof(operationId));

    SemPost(&ptr->worker, __func__);
    SemPost(&ptr->mutex, __func__);

    SemWait(&ptr->responseSync[responseId], __func__);

    current = ResponseQueue[responseId];
    memcpy(bufLen, current, sizeof(*bufLen));
    current += sizeof(*bufLen);
    bool hasNext;
    memcpy(&hasNext, current, sizeof(hasNext));

    SemPost(&ptr->responseMutex[responseId], __func__);

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
                SharedMem *ptr,
                char *key,
                size_t keyLen,
                char **val,
                size_t *valLen) {
//    printf("\n============%s============\n", __func__);

    SemWait(&ptr->mutex, __func__);
    SemWait(&ptr->full, __func__);

    char *current = ptr->area;
    FuncName func = GET;
    memcpy(current, &func, sizeof(func));
    current += sizeof(func);

    uint32 responseId = GetResponseQueueIndex(ptr);
    memcpy(current, &responseId, sizeof(responseId));
    current += sizeof(responseId);

    memcpy(current, &relationId, sizeof(relationId));
    current += sizeof(relationId);

    memcpy(current, &keyLen, sizeof(keyLen));
    current += sizeof(keyLen);

    memcpy(current, key, keyLen);

    SemPost(&ptr->worker, __func__);
    SemPost(&ptr->mutex, __func__);
    SemWait(&ptr->responseSync[responseId], __func__);

    current = ResponseQueue[responseId];
    bool res;
    memcpy(&res, current, sizeof(res));
    if (!res) {
        SemPost(&ptr->responseMutex[responseId], __func__);
        return false;
    }

    current += sizeof(res);
    memcpy(valLen, current, sizeof(*valLen));
    current += sizeof(*valLen);

    *val = palloc(*valLen);
    memcpy(*val, current, *valLen);

    SemPost(&ptr->responseMutex[responseId], __func__);

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
                SharedMem *ptr,
                char *key,
                size_t keyLen,
                char *val,
                size_t valLen) {
//    printf("\n============%s============\n", __func__);

    SemWait(&ptr->mutex, __func__);
    SemWait(&ptr->full, __func__);

    char *current = ptr->area;
    FuncName func = PUT;
    memcpy(current, &func, sizeof(func));
    current += sizeof(func);

    uint32 responseId = GetResponseQueueIndex(ptr);
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

    if (current - ptr->area > BUFSIZE) {
        SemPost(&ptr->mutex, __func__);
        SemPost(&ptr->responseMutex[responseId], __func__);
        ereport(ERROR,
                (errmsg("%s tuple is too long, increase BUFSIZE", __func__)));
    }

    SemPost(&ptr->worker, __func__);
    SemPost(&ptr->mutex, __func__);

    SemWait(&ptr->responseSync[responseId], __func__);
    SemPost(&ptr->responseMutex[responseId], __func__);
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

    size_t *valLen = (size_t *)valLen;
    area += sizeof(*valLen);

    char *val = area;
    if (!Put(entry->db, key, *keyLen, val, *valLen)) {
        ereport(ERROR, (errmsg("error from %s", __func__)));
    }
}

void DeleteRequest(Oid relationId, SharedMem *ptr, char *key, size_t keyLen) {
//    printf("\n============%s============\n", __func__);

    SemWait(&ptr->mutex, __func__);
    SemWait(&ptr->full, __func__);

    char *current = ptr->area;
    FuncName func = DELETE;
    memcpy(current, &func, sizeof(func));
    current += sizeof(func);

    uint32 responseId = GetResponseQueueIndex(ptr);
    memcpy(current, &responseId, sizeof(responseId));
    current += sizeof(responseId);

    memcpy(current, &relationId, sizeof(relationId));
    current += sizeof(relationId);

    memcpy(current, &keyLen, sizeof(keyLen));
    current += sizeof(keyLen);

    memcpy(current, key, keyLen);

    SemPost(&ptr->worker, __func__);
    SemPost(&ptr->mutex, __func__);

    SemWait(&ptr->responseSync[responseId], __func__);
    SemPost(&ptr->responseMutex[responseId], __func__);
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
                       SharedMem *ptr,
                       RangeQueryOptions *options,
                       char **buf,
                       size_t *bufLen) {
//    printf("\n============%s============\n", __func__);

    /* munmap the shared memory so that Response can unlink it */
    if (*buf && *bufLen > 0) {
        Munmap(*buf, *bufLen, __func__);
    }

    SemWait(&ptr->mutex, __func__);
    SemWait(&ptr->full, __func__);

    char *current = ptr->area;
    FuncName func = RANGEQUERY;
    memcpy(current, &func, sizeof(func));
    current += sizeof(func);

    uint32 responseId = GetResponseQueueIndex(ptr);
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

    SemPost(&ptr->worker, __func__);
    SemPost(&ptr->mutex, __func__);
    SemWait(&ptr->responseSync[responseId], __func__);

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

    SemPost(&ptr->responseMutex[responseId], __func__);
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
            options.start = area;
            area += options.startLen;
        }

        options.limitLen = *((size_t *)area);
        area += sizeof(options.limitLen);
        if (options.limitLen > 0) {
            options.limit = area;
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
                                SharedMem *ptr,
                                TableReadState *readState) {
//    printf("\n============%s============\n", __func__);

    if (readState->buf && readState->bufLen > 0) {
        Munmap(readState->buf, readState->bufLen, __func__);
    }

    SemWait(&ptr->mutex, __func__);
    SemWait(&ptr->full, __func__);

    char *current = ptr->area;
    FuncName func = CLEARRQMETA;
    memcpy(current, &func, sizeof(func));
    current += sizeof(func);

    uint32 responseId = GetResponseQueueIndex(ptr);
    memcpy(current, &responseId, sizeof(responseId));
    current += sizeof(responseId);

    memcpy(current, &relationId, sizeof(relationId));
    current += sizeof(relationId);

    pid_t pid = getpid();
    memcpy(current, &pid, sizeof(pid));
    current += sizeof(pid);

    memcpy(current, &operationId, sizeof(operationId));

    SemPost(&ptr->worker, __func__);
    SemPost(&ptr->mutex, __func__);

    SemWait(&ptr->responseSync[responseId], __func__);
    SemPost(&ptr->responseMutex[responseId], __func__);
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
