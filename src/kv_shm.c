
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

typedef struct KVIterHashKey {
    Oid relationId;
    pid_t pid;
} KVIterHashKey;

typedef struct KVIterHashEntry {
    KVIterHashKey key;
    void *iter;
} KVIterHashEntry;


pid_t kvWorkerPid = 0;  // in postmaster process

HTAB *kvTableHash = NULL;  // in kvworker process

HTAB *kvIterHash = NULL;

long HASHSIZE = 1;  // non-shared hash can be enlarged

char *ResponseQueue[RESPONSEQUEUELENGTH];

static int StartKVWorker(void);

static void OpenResponse(char *area);

static void CloseResponse(char *area);

static void CountResponse(char *area);

static void GetIterResponse(char *area);

static void DelIterResponse(char *area);

static void NextResponse(char *area);

static void GetResponse(char *area);

static void PutResponse(char *area);

static void DeleteResponse(char *area);

void InitResponseArea(void);

void OpenResponseArea(void);

int CompareKVIterHashKey(const void *key1, const void *key2, Size keysize);

static void cleanup_handler(void *arg) {
    printf("\n============%s============\n", __func__);
    SharedMem *ptr = *((SharedMem **)arg);

    if (kvWorkerPid != 0) {
        FuncName func = TERMINATE;
        memcpy(ptr->area, &func, sizeof(FuncName));
        SemPost(&ptr->worker[0], __func__);
        SemWait(&ptr->worker[1], __func__);
        kvWorkerPid = 0;
    }

    // release the response area first
    char filename[FILENAMELENGTH];
    for (int i = 0; i < RESPONSEQUEUELENGTH; i++) {
        //SemWait(&ptr->responseMutexes[i], __func__);
        Munmap(ResponseQueue[i], DATAAREASIZE, __func__);
        snprintf(filename, FILENAMELENGTH, "%s%d", RESPONSEFILE, i);
        ShmUnlink(filename, __func__);
    }
    
    SemDestroy(&ptr->mutex, __func__);
    SemDestroy(&ptr->full, __func__);
    SemDestroy(&ptr->agent[0], __func__);
    SemDestroy(&ptr->agent[1], __func__);
    SemDestroy(&ptr->worker[0], __func__);
    SemDestroy(&ptr->worker[1], __func__);

    for (int i = 0; i < RESPONSEQUEUELENGTH; i++) {
        SemDestroy(&ptr->responseMutexes[i], __func__);
    }
    

    Munmap(ptr, sizeof(SharedMem), __func__);
    ShmUnlink(BACKFILE, __func__);
}

/*
 * Initialize shared memory for responses
 */
void InitResponseArea(){
    char filename[FILENAMELENGTH];
    for (int i = 0; i < RESPONSEQUEUELENGTH; i++) {
        snprintf(filename, FILENAMELENGTH, "%s%d", RESPONSEFILE, i);
        ShmUnlink(filename, __func__);
        int fd = ShmOpen(filename, O_CREAT | O_RDWR | O_EXCL, PERMISSION, __func__);
        ResponseQueue[i] = Mmap(NULL, 
                          DATAAREASIZE, 
                          PROT_READ | PROT_WRITE, 
                          MAP_SHARED, 
                          fd, 
                          0, 
                          __func__);
        Ftruncate(fd, DATAAREASIZE, __func__);
        Fclose(fd, __func__);
    }  
}

/*
 * Open shared memory for responses
 */
void OpenResponseArea() {
    char filename[FILENAMELENGTH];
    for (int i = 0; i < RESPONSEQUEUELENGTH; i++) {
        if (ResponseQueue[i] == NULL) {
            snprintf(filename, FILENAMELENGTH, "%s%d", RESPONSEFILE, i);
            int fd = ShmOpen(filename, O_RDWR, PERMISSION, __func__);
            ResponseQueue[i] = Mmap(NULL,
                              DATAAREASIZE,
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
/* int CompareKVIterHashKey(const void *key1, const void *key2, Size keysize) {
    KVIterHashKey *k1 = (const KVIterHashKey *)key1;
    KVIterHashKey *k2 = (const KVIterHashKey *)key2;

    if(k1 == NULL || k2 == NULL) {
        return -1;
    }

    if(k1->relationId == k2->relationId && k1->pid == k2->pid) {
        return 0;
    }

    return -1;
}*/

void *KVStorageThreadFun(void *arg) {
    PthreadSetCancelState(PTHREAD_CANCEL_ENABLE, NULL, __func__);
    PthreadSetCancelType(PTHREAD_CANCEL_DEFERRED, NULL, __func__);

    SharedMem *ptr = NULL;
    pthread_cleanup_push(cleanup_handler, &ptr);

    ShmUnlink(BACKFILE, __func__);
    int fd = ShmOpen(BACKFILE, O_CREAT | O_RDWR | O_EXCL, PERMISSION, __func__);
    ptr = Mmap(NULL, sizeof(SharedMem), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0, __func__);
    Ftruncate(fd, sizeof(SharedMem), __func__);
    Fclose(fd, __func__);

    //Initialize the response area
    InitResponseArea();

    SemInit(&ptr->mutex, 1, 1, __func__);
    SemInit(&ptr->full, 1, 1, __func__);
    SemInit(&ptr->agent[0], 1, 0, __func__);
    SemInit(&ptr->agent[1], 1, 0, __func__);
    SemInit(&ptr->worker[0], 1, 0, __func__);
    SemInit(&ptr->worker[1], 1, 0, __func__);
    for (int i = 0; i < RESPONSEQUEUELENGTH; i++) {
        SemInit(&ptr->responseMutexes[i], 1, 1, __func__);
    }
    
    ptr->workerProcessCreated = false;

    do {
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
                          sizeof(SharedMem),
                          PROT_READ | PROT_WRITE,
                          MAP_SHARED,
                          fd,
                          0,
                          __func__);
    Fclose(fd, __func__);

    //open the response queue
    OpenResponseArea();

    HASHCTL hash_ctl;
    memset(&hash_ctl, 0, sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(Oid);
    hash_ctl.entrysize = sizeof(KVHashEntry);
    kvTableHash = hash_create("kvTableHash",
                              HASHSIZE,
                              &hash_ctl,
                              HASH_ELEM | HASH_BLOBS);

    HASHCTL iterhash_ctl;
    memset(&iterhash_ctl, 0, sizeof(iterhash_ctl));
    iterhash_ctl.keysize = sizeof(KVIterHashKey);
    iterhash_ctl.entrysize = sizeof(KVIterHashEntry);
    //iterhash_ctl.match = CompareKVIterHashKey;
    kvIterHash = hash_create("kvIterHash",
                             HASHSIZE,
                             &iterhash_ctl,
                             HASH_ELEM | HASH_BLOBS);

    char buff[DATAAREASIZE];
    do {
        SemWait(&ptr->worker[0], __func__);

        FuncName func;
        memcpy(&func, ptr->area, sizeof(FuncName));
        memset(buff, 0, DATAAREASIZE);
        memcpy(buff, ptr->area + sizeof(FuncName), DATAAREASIZE - sizeof(FuncName));
 
        if (func == TERMINATE) {
            SemPost(&ptr->worker[1], __func__);
            break;
        }

        switch (func) {
            case OPEN:
                OpenResponse(buff);
                break;
            case CLOSE:
                CloseResponse(buff);
                break;
            case COUNT:
                CountResponse(buff);
                break;
            case GETITER:
                GetIterResponse(buff);
                break;
            case DELITER:
                DelIterResponse(buff);
                break;
            case NEXT:
                NextResponse(buff);
                break;
            case GET:
                GetResponse(buff);
                break;
            case PUT:
                PutResponse(buff);
                break;
            case DELETE:
                DeleteResponse(buff);
                break;
            default:
                ereport(ERROR, (errmsg("%s failed in switch", __func__)));
        }

        SemPost(&ptr->worker[1], __func__);
        SemPost(&ptr->full, __func__);
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
            printf("========================kvworker %d ======================", kvWorkerPid);
            return (int) kvWorkerPid;
    }

    /* shouldn't get here */
    return 0;
}

/*
 * A child process must acquire the mutex of the shared memory before calling this functions,
 * so processes check the available response slot in the FIFO manner.
 * If all the response slots are used by other processes, the caller process will loop here. 
 */
inline int GetResponseQueueIndex(SharedMem *ptr) {
    while (true) {
        for (int i = 0; i < RESPONSEQUEUELENGTH; i++) {
            int ret = SemTryWait(&ptr->responseMutexes[i], __func__);
            if (ret == 0) {
                return i;
            }
        }
    }
}

SharedMem *OpenRequest(Oid relationId, SharedMem *ptr) {
    //printf("\n============%s============\n", __func__);

    if (!ptr) {
        int fd = ShmOpen(BACKFILE, O_RDWR, PERMISSION, __func__);
        ptr = Mmap(NULL,
                   sizeof(SharedMem),
                   PROT_READ | PROT_WRITE,
                   MAP_SHARED,
                   fd,
                   0,
                   __func__);
        Fclose(fd, __func__);

        OpenResponseArea();
    }    

    //lock among child processes
    SemWait(&ptr->mutex, __func__);
    
    if (!ptr->workerProcessCreated) {
        SemPost(&ptr->agent[0], __func__);
        SemWait(&ptr->agent[1], __func__);
    }

    // wait for the worker process copy out the previous request
    SemWait(&ptr->full, __func__);
    // open request does not need a response
    FuncName func = OPEN;
    memcpy(ptr->area, &func, sizeof(FuncName));

    KVFdwOptions *fdwOptions = KVGetOptions(relationId);
    char *path = fdwOptions->filename;
    strcpy(ptr->area + sizeof(FuncName), path);
    
    SemPost(&ptr->worker[0], __func__);
    //unlock
    SemPost(&ptr->mutex, __func__);
 
    SemWait(&ptr->worker[1], __func__);

    return ptr;
}

static void OpenResponse(char *area) {
    //printf("\n============%s============\n", __func__);

    char path[PATHMAXLENGTH];
    strcpy(path, area);
    char *pos = strrchr(path, '/');
    Oid relationId = atoi(pos + 1);
    bool found;
    KVHashEntry *entry = hash_search(kvTableHash, &relationId, HASH_ENTER, &found);
    if (!found) {
        entry->relationId = relationId;
        entry->ref = 1;
        entry->db = Open(path);
    } else {
        entry->ref++;
        printf("\n%s ref %d\n", __func__, entry->ref);
    }
}

void CloseRequest(Oid relationId, SharedMem *ptr) {
    //printf("\n============%s============\n", __func__);

    SemWait(&ptr->mutex, __func__);
    SemWait(&ptr->full, __func__);
    FuncName func = CLOSE;
    memcpy(ptr->area, &func, sizeof(FuncName));
    memcpy(ptr->area + sizeof(FuncName), &relationId, sizeof(relationId));
    SemPost(&ptr->worker[0], __func__);
    SemPost(&ptr->mutex, __func__);

    SemWait(&ptr->worker[1], __func__);
}

static void CloseResponse(char *area) {
    //printf("\n============%s============\n", __func__);

    Oid relationId;
    memcpy(&relationId, area, sizeof(relationId));

    bool found;
    KVHashEntry *entry = hash_search(kvTableHash, &relationId, HASH_FIND, &found);
    if (!found) {
        ereport(ERROR, (errmsg("%s failed in hash search", __func__)));
    } else {
        entry->ref--;
        printf("\n%s ref %d\n", __func__, entry->ref);
    }
}

uint64 CountRequest(Oid relationId, SharedMem *ptr) {
    //printf("\n============%s============\n", __func__);

    SemWait(&ptr->mutex, __func__);
    SemWait(&ptr->full, __func__);
    
    FuncName func = COUNT;
    memcpy(ptr->area, &func, sizeof(FuncName));
    int response_idx = GetResponseQueueIndex(ptr);
  
    memcpy(ptr->area + sizeof(FuncName), &response_idx, sizeof(int));
    memcpy(ptr->area + sizeof(FuncName) + sizeof(int), &relationId, sizeof(relationId));
    SemPost(&ptr->worker[0], __func__);
    SemPost(&ptr->mutex, __func__);

    
    uint64 count;
    SemWait(&ptr->worker[1], __func__);
    
    char *response = ResponseQueue[response_idx];
    memcpy(&count, response, sizeof(uint64));
    //memcpy(&count, ptr->area + sizeof(FuncName) + sizeof(int), sizeof(uint64));
    SemPost(&ptr->responseMutexes[response_idx], __func__);
    return count;
}

static void CountResponse(char *area) {
    //printf("\n============%s============\n", __func__);

    int response_index;
    memcpy(&response_index, area, sizeof(int));
    Oid relationId;
    memcpy(&relationId, area + sizeof(int), sizeof(relationId));

    bool found;
    KVHashEntry *entry = hash_search(kvTableHash, &relationId, HASH_FIND, &found);
    if (!found) {
        ereport(ERROR, (errmsg("%s failed in hash search", __func__)));
    } else {
        uint64 count = Count(entry->db);
        //memcpy(area, &count, sizeof(uint64));
        memcpy(ResponseQueue[response_index], &count, sizeof(uint64));
    }
}

void GetIterRequest(Oid relationId, SharedMem *ptr) {
    //printf("\n============%s============\n", __func__);

    SemWait(&ptr->mutex, __func__);

    SemWait(&ptr->full, __func__);
    FuncName func = GETITER;
    memcpy(ptr->area, &func, sizeof(FuncName)); 
    memcpy(ptr->area + sizeof(FuncName), &relationId, sizeof(relationId));
    pid_t pid = getpid();
    printf("\n============%s %d============\n", __func__, pid);
    memcpy(ptr->area + sizeof(FuncName) + sizeof(relationId), &pid, sizeof(pid));
    SemPost(&ptr->worker[0], __func__);
    SemPost(&ptr->mutex, __func__);
    SemWait(&ptr->worker[1], __func__);
}

static void GetIterResponse(char *area) {
    //printf("\n============%s============\n", __func__);

    KVIterHashKey iterKey;    
    memcpy(&iterKey.relationId, area, sizeof(iterKey.relationId));
    memcpy(&iterKey.pid, area + sizeof(iterKey.relationId), sizeof(pid_t));

    bool found;
    KVHashEntry *entry = hash_search(kvTableHash, &iterKey.relationId, HASH_FIND, &found);
 
    if (!found) {
        ereport(ERROR, (errmsg("%s failed in hash search", __func__)));
    } else {
        bool foundIter;
        KVIterHashEntry *iterEntry = hash_search(kvIterHash, &iterKey, HASH_ENTER, &foundIter);    
        if(!foundIter) {
            iterEntry->key = iterKey;
        }

        iterEntry->iter = GetIter(entry->db);
    }
}

void DelIterRequest(Oid relationId, SharedMem *ptr) {
    //printf("\n============%s============\n", __func__);
    
    SemWait(&ptr->mutex, __func__);

    SemWait(&ptr->full, __func__);
    FuncName func = DELITER;
    memcpy(ptr->area, &func, sizeof(FuncName));
    memcpy(ptr->area + sizeof(FuncName), &relationId, sizeof(relationId));
    pid_t pid = getpid();
    memcpy(ptr->area + sizeof(FuncName) + sizeof(relationId), &pid, sizeof(pid_t));
    SemPost(&ptr->worker[0], __func__);
    SemPost(&ptr->mutex, __func__);
    SemWait(&ptr->worker[1], __func__);
}

static void DelIterResponse(char *area) {
    //printf("\n============%s============\n", __func__);

    KVIterHashKey iterKey;
    memcpy(&iterKey.relationId, area, sizeof(iterKey.relationId));
    memcpy(&iterKey.pid, area + sizeof(iterKey.relationId), sizeof(pid_t));

    bool found;
    KVIterHashEntry *entry = hash_search(kvIterHash, &iterKey, HASH_FIND, &found);
    if (!found) {
        ereport(ERROR, (errmsg("%s failed in hash search", __func__)));
    } else {
        if (entry->iter) {
            DelIter(entry->iter);
            entry->iter = NULL;
        }
    }
}

bool NextRequest(Oid relationId,
                 SharedMem *ptr,
                 char** key,
                 uint32* keyLen,
                 char** val,
                 uint32* valLen) {
    //printf("\n============%s============\n", __func__);
      
    SemWait(&ptr->mutex, __func__);
    SemWait(&ptr->full, __func__);

    FuncName func = NEXT;
    memcpy(ptr->area, &func, sizeof(FuncName));
    
    int response_idx = GetResponseQueueIndex(ptr);
    memcpy(ptr->area + sizeof(FuncName), &response_idx, sizeof(int));
    
    memcpy(ptr->area + sizeof(FuncName) + sizeof(int), &relationId, sizeof(relationId));
    pid_t pid = getpid();
    memcpy(ptr->area + sizeof(FuncName) + sizeof(int) + sizeof(relationId), &pid, sizeof(pid));
    
    SemPost(&ptr->worker[0], __func__);
    SemPost(&ptr->mutex, __func__);
    SemWait(&ptr->worker[1], __func__);

    
    //char *current = ptr->area + sizeof(FuncName);
    char *current = ResponseQueue[response_idx];
    memcpy(keyLen, current, sizeof(*keyLen));

    /* no next item */
    if (*keyLen == 0) {
        SemPost(&ptr->responseMutexes[response_idx], __func__);
        return false;
    }

    current += sizeof(*keyLen);
    //printf("\n============%s %d %d p1============\n", __func__, response_idx, *keyLen);
    *key = (char*) palloc0(*keyLen);
    memcpy(*key, current, *keyLen);

    current += *keyLen;
    memcpy(valLen, current, sizeof(*valLen));

    current += sizeof(*valLen);
    printf("\n============%s %d p2============\n", __func__, *valLen);
    *val = (char*) palloc0(*valLen);
    memcpy(*val, current, *valLen);

    SemPost(&ptr->responseMutexes[response_idx], __func__);
    return true;
}

void NextResponse(char *area) {
    //printf("\n============%s============\n", __func__);

    int response_index;
    memcpy(&response_index, area, sizeof(int));

    KVIterHashKey iterKey;
    memcpy(&iterKey.relationId, area + sizeof(int), sizeof(iterKey.relationId));
    memcpy(&iterKey.pid, area + sizeof(int) + sizeof(iterKey.relationId), sizeof(pid_t));

   
    bool found;
    KVHashEntry *entry = hash_search(kvTableHash, &iterKey.relationId, HASH_FIND, &found);
    if (!found) {
        ereport(ERROR, (errmsg("%s failed in hash search", __func__)));
    } else {
        bool iterFound;
        KVIterHashEntry *iterEntry = hash_search(kvIterHash, &iterKey, HASH_FIND, &iterFound);
        if (!iterFound)
        {
            ereport(ERROR, (errmsg("%s failed in hash search for iterator", __func__)));
        }else
        {
            char *key = NULL, *val = NULL;
            uint32 keyLen = 0, valLen = 0;
   
            bool res = Next(entry->db, iterEntry->iter, &key, &keyLen, &val, &valLen);
            
            if (!res) {
                /* no next item */
                //memcpy(area, &keyLen, sizeof(keyLen));
                memset(ResponseQueue[response_index], 0, sizeof(keyLen));
                memcpy(ResponseQueue[response_index], &keyLen, sizeof(keyLen));
            
                return;
            }

            //printf("\n============%s gaomao %d %d ============\n", __func__, response_index, keyLen);
            //char *current = area;
            memset(ResponseQueue[response_index], 0, DATAAREASIZE);
            char *current = ResponseQueue[response_index];
            memcpy(current, &keyLen, sizeof(keyLen));

            current += sizeof(keyLen);
            memcpy(current, key, keyLen);

            current += keyLen;
            memcpy(current, &valLen, sizeof(valLen));

            current += sizeof(valLen);
            memcpy(current, val, valLen);

            pfree(key);
            pfree(val);
        }
    }
}

bool GetRequest(Oid relationId,
                SharedMem *ptr,
                char* key,
                uint32 keyLen,
                char** val,
                uint32* valLen) {
    //printf("\n============%s============\n", __func__);

    SemWait(&ptr->mutex, __func__);
    SemWait(&ptr->full, __func__);

    FuncName func = GET;
    memcpy(ptr->area, &func, sizeof(FuncName));

    int response_idx = GetResponseQueueIndex(ptr);
    memcpy(ptr->area + sizeof(FuncName), &response_idx, sizeof(int));

    memcpy(ptr->area + sizeof(FuncName) + sizeof(int), &relationId, sizeof(relationId));

    char *current = ptr->area + sizeof(FuncName) + sizeof(int) + sizeof(relationId);
    memcpy(current, &keyLen, sizeof(keyLen));

    current += sizeof(keyLen);
    memcpy(current, key, keyLen);

    SemPost(&ptr->worker[0], __func__);
    SemPost(&ptr->mutex, __func__);
    SemWait(&ptr->worker[1], __func__);

    //current = ptr->area + sizeof(FuncName);
    current = ResponseQueue[response_idx];
    bool res;
    memcpy(&res, current, sizeof(bool));
    if (!res) {
        SemPost(&ptr->responseMutexes[response_idx], __func__);
        return false;
    }

    current += sizeof(bool);
    memcpy(valLen, current, sizeof(*valLen));

    current += sizeof(*valLen);
    
    *val = (char*) palloc0(*valLen);
    memcpy(*val, current, *valLen);

    SemPost(&ptr->responseMutexes[response_idx], __func__);
    
    return true;
}

static void GetResponse(char *area) {
    //printf("\n============%s============\n", __func__);

    int response_index;
    memcpy(&response_index, area, sizeof(int));

    Oid relationId;
    memcpy(&relationId, area + sizeof(int), sizeof(relationId));

    bool found;
    KVHashEntry *entry = hash_search(kvTableHash, &relationId, HASH_FIND, &found);
    if (!found) {
        ereport(ERROR, (errmsg("%s failed in hash search", __func__)));
    } else {
        uint32 keyLen, valLen;
        char *current = area + sizeof(int) + sizeof(relationId);

        memcpy(&keyLen, current, sizeof(keyLen));
        char *key = current + sizeof(keyLen);

        char *val = NULL;
        bool res = Get(entry->db, key, keyLen, &val, &valLen);
        //memcpy(area, &res, sizeof(bool));
        memcpy(ResponseQueue[response_index], &res, sizeof(bool));
        if (!res) {
            return;
        }

        //current = area + sizeof(bool);
        current = ResponseQueue[response_index] + sizeof(bool);
        memset(current, 0, sizeof(valLen));
        memcpy(current, &valLen, sizeof(valLen));

        current += sizeof(valLen);
        memset(current, 0, valLen);
        memcpy(current, val, valLen);

        pfree(val);
    }
}

void PutRequest(Oid relationId,
                SharedMem *ptr,
                char* key,
                uint32 keyLen,
                char* val,
                uint32 valLen) {
    //printf("\n============%s============\n", __func__);

    SemWait(&ptr->mutex, __func__);
    SemWait(&ptr->full, __func__);

    FuncName func = PUT;
    memcpy(ptr->area, &func, sizeof(FuncName));

    memcpy(ptr->area + sizeof(FuncName), &relationId, sizeof(relationId));

    char *current = ptr->area + sizeof(FuncName) + sizeof(relationId);
    memcpy(current, &keyLen, sizeof(keyLen));

    current += sizeof(keyLen);
    memcpy(current, key, keyLen);

    current += keyLen;
    memcpy(current, &valLen, sizeof(valLen));

    current += sizeof(valLen);
    memcpy(current, val, valLen);

    SemPost(&ptr->worker[0], __func__);
    SemPost(&ptr->mutex, __func__);
    SemWait(&ptr->worker[1], __func__);
}

static void PutResponse(char *area) {
    //printf("\n============%s============\n", __func__);

    Oid relationId;
    memcpy(&relationId, area, sizeof(relationId));

    bool found;
    KVHashEntry *entry = hash_search(kvTableHash, &relationId, HASH_FIND, &found);
    if (!found) {
        ereport(ERROR, (errmsg("%s failed in hash search", __func__)));
    } else {
        uint32 keyLen, valLen;
        char *current = area + sizeof(relationId);

        memcpy(&keyLen, current, sizeof(keyLen));
        char *key = current + sizeof(keyLen);

        current += sizeof(keyLen) + keyLen;
        memcpy(&valLen, current, sizeof(valLen));
        char *val = current + sizeof(valLen);

        if (!Put(entry->db, key, keyLen, val, valLen)) {
            ereport(ERROR, (errmsg("error from %s", __func__)));
        }
    }
}

void DeleteRequest(Oid relationId, SharedMem *ptr, char* key, uint32 keyLen) {
    //printf("\n============%s============\n", __func__);

    SemWait(&ptr->mutex, __func__);
    SemWait(&ptr->full, __func__);
    FuncName func = DELETE;
    memcpy(ptr->area, &func, sizeof(FuncName));
    memcpy(ptr->area + sizeof(FuncName), &relationId, sizeof(relationId));

    char *current = ptr->area + sizeof(FuncName) + sizeof(relationId);
    memcpy(current, &keyLen, sizeof(keyLen));

    current += sizeof(keyLen);
    memcpy(current, key, keyLen);

    SemPost(&ptr->worker[0], __func__);
    SemPost(&ptr->mutex, __func__);
    SemWait(&ptr->worker[1], __func__);
}

static void DeleteResponse(char *area) {
    //printf("\n============%s============\n", __func__);

    Oid relationId;
    memcpy(&relationId, area, sizeof(relationId));

    bool found;
    KVHashEntry *entry = hash_search(kvTableHash, &relationId, HASH_FIND, &found);
    if (!found) {
        ereport(ERROR, (errmsg("%s failed in hash search", __func__)));
    } else {
        uint32 keyLen;
        char *current = area + sizeof(relationId);

        memcpy(&keyLen, current, sizeof(keyLen));
        char *key = current + sizeof(keyLen);

        if (!Delete(entry->db, key, keyLen)) {
            ereport(ERROR, (errmsg("error from %s", __func__)));
        }
    }
}
