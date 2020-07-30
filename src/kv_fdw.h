
#ifndef KV_FDW_H_
#define KV_FDW_H_


#include "kv_storage.h"

#include <stdbool.h>
#include <semaphore.h>

#include "postgres.h"
#include "lib/stringinfo.h"
#include "nodes/nodes.h"
#include "access/attnum.h"
#include "utils/relcache.h"
#include "utils/hsearch.h"


/* Defines */
#define KVFDWNAME "kv_fdw"

#define BACKFILE "/KVSharedMem"

#define PERMISSION 0777

#define PATHMAXLENGTH 4096

#define FILENAMELENGTH 64

#define BUFSIZE 65536

#define RESPONSEFILE "/KVSharedResponse"

#define RESPONSEQUEUELENGTH 2

#define HEADERBUFFSIZE 10

#define READBATCHSIZE 4096*20

#define READBATCHFILE "/KVReadBatch"


/* Defines for valid options and the default values */
#define OPTION_FILENAME "filename"

#ifdef VIDARDB
#define OPTION_STORAGE_FORMAT "storage"

#define OPTION_BATCH_CAPACITY "batch"

#define COLUMNSTORE "column"

#define BATCHCAPACITY 8*1024*1024

#define RANGEQUERYFILE "/KVRangeQuery"
#endif


/* Defines for load operation */
#define LOADFILE "/KVLoad"

#define LOADBUFSIZE 65536

/*
 * Common functions for communication with worker process.
 */
typedef enum FuncName {
    OPEN = 0,
    CLOSE,
    COUNT,
    GETITER,
    DELITER,
    READBATCH,
    GET,
    PUT,
    DELETE,
    #ifdef VIDARDB
    RANGEQUERY,
    CLEARRQMETA,
    #endif
    LOAD,
    TERMINATE
} FuncName;

/* Shared memory for communication with manager:
 * mutex: manager serves only one backend at a time;
 * manager, backend: coordinate between the two roles;
 * ready: to avoid race between worker and backend at worker init;
 * databaseId: backend tells manager the database it wants;
 * relationId: backend tells manager the relation it wants;
 * func: backend tells manager the action it wants;
 * success: manager tells backend the action result;
 */
typedef struct ManagerSharedMem {
    sem_t mutex;
    sem_t manager;
    sem_t backend;
    sem_t ready;
    Oid databaseId;
    Oid relationId;
    FuncName func;
    volatile bool success;
} ManagerSharedMem;

/* Shared memory for function requests with worker:
 * mutex: mutual exclusion of the request buffer;
 * full: tell whether the request buffer is full;
 * worker: notify the worker process after a request is submitted;
 * responseMutexes[RESPONSEQUEUELENGTH]:
 *     mutual exclusion of the response buffer;
 * responseSync[RESPONSEQUEUELENGTH]:
 *     notify child processes after the response is ready.
 */
typedef struct WorkerSharedMem {
    sem_t mutex;
    sem_t full;
    sem_t worker;
    sem_t responseMutex[RESPONSEQUEUELENGTH];
    sem_t responseSync[RESPONSEQUEUELENGTH];
    char area[BUFSIZE];  /* assume ~64K for a tuple is enough */
} WorkerSharedMem;

/* Composite key for worker process:
 * databaseId: the database related by worker;
 * relationId: the relation related by worker;
 */
typedef struct WorkerProcKey {
    Oid databaseId;
    Oid relationId;
} WorkerProcKey;

/* Key must be the first attribute */
typedef struct WorkerProcShmEntry {
    WorkerProcKey key;
    WorkerSharedMem *shm;
} WorkerProcShmEntry;

/* Ring buffer shared memory for load operation:
 * mutex: mutual exclusion for offset;
 * empty: tell whether the buffer is empty;
 * full: tell whether the buffer is full;
 * done: tell whether worker has put all data;
 * in: the offset producer can put data;
 * out: the offset consumer can get data;
 * count: record the inserted row number;
 * finish: tell whether has read all data;
 * area: the temporary data storage area;
 */
typedef struct RingBufSharedMem {
    sem_t mutex;
    sem_t empty;
    sem_t full;
    sem_t done;
    volatile uint64 in;
    volatile uint64 out;
    volatile uint64 count;
    volatile bool finish;
    char area[LOADBUFSIZE];  /* assume ~64K for a tuple is enough */
} RingBufSharedMem;

/* Holds the option values to be used when reading or writing files.
 * To resolve these values, we first check foreign table's options,
 * and if not present, we then fall back to the default values.
 */
typedef struct KVFdwOptions {
    char *filename;

    #ifdef VIDARDB
    bool useColumn;
    int32 batchCapacity;
    #endif
} KVFdwOptions;

#ifdef VIDARDB
typedef struct TablePlanState {
    KVFdwOptions *fdwOptions;
    int attrCount;        /* total attributes in a table */
    List *targetAttrs;    /* attributes in select, where, groupby */
    bool toUpdateDelete;  /* any update or delete? indicate when to delete */
} TablePlanState;
#endif

/*
 * The scan state is for maintaining state for a scan, either for a
 * SELECT or UPDATE or DELETE.
 *
 * It is set up in BeginForeignScan and stashed in node->fdw_state and
 * subsequently used in IterateForeignScan, EndForeignScan and ReScanForeignScan.
 */
typedef struct TableReadState {
    bool isKeyBased;
    uint64 operationId;
    bool done;
    StringInfo key;
    char *buf;     /* shared mem for data returned by RangeQuery or ReadBatch */
    size_t bufLen; /* shared mem length, no next batch if it is 0 */
    char *next;    /* pointer to the next data entry for IterateForeignScan */
    bool hasNext;  /* whether a next batch from RangeQuery or ReadBatch*/

    #ifdef VIDARDB
    bool useColumn;
    List *targetAttrs;    /* attributes in select, where, group */
    #endif
} TableReadState;

/*
 * The modify state is for maintaining state of modify operations.
 *
 * It is set up in BeginForeignModify and stashed in
 * rinfo->ri_FdwState and subsequently used in ExecForeignInsert,
 * ExecForeignUpdate, ExecForeignDelete and EndForeignModify.
 */
typedef struct TableWriteState {
    CmdType operation;
} TableWriteState;


/* Function declarations for extension loading and unloading */
extern void _PG_init(void);

extern void _PG_fini(void);

#ifdef VIDARDB
/* Fill the specified relation's comparator options */
extern void FillRelationComparatorOptions(Relation relation,
                                          ComparatorOptions *opts);
#endif

/* Functions used across files in kv_fdw */
extern KVFdwOptions *KVGetOptions(Oid foreignTableId);

extern void SerializeNullAttribute(TupleDesc tupleDescriptor,
                                   Index index,
                                   StringInfo buffer);

extern void SerializeAttribute(TupleDesc tupleDescriptor,
                               Index index,
                               Datum datum,
                               StringInfo buffer);

extern char *KVGetOptionValue(Oid foreignTableId, const char *optionName);

extern Datum ShortVarlena(Datum datum, int typeLength, char storage);

extern WorkerSharedMem *OpenRequest(Oid relationId,
                                    ManagerSharedMem **managerPtr,
                                    HTAB **workerShmHashPtr, ...);

extern void CloseRequest(Oid relationId, WorkerSharedMem *worker);

extern uint64 CountRequest(Oid relationId, WorkerSharedMem *worker);

extern void GetIterRequest(Oid relationId,
                           uint64 operationId,
                           WorkerSharedMem *worker);

extern void DelIterRequest(Oid relationId,
                           uint64 operationId,
                           WorkerSharedMem *worker,
                           TableReadState *readState);

extern bool NextRequest(Oid relationId,
                        uint64 operationId,
                        WorkerSharedMem *worker,
                        char **key,
                        size_t *keyLen,
                        char **val,
                        size_t *valLen);

extern bool ReadBatchRequest(Oid relationId,
                             uint64 operationId,
                             WorkerSharedMem *worker,
                             char **buf,
                             size_t *bufLen);

extern bool GetRequest(Oid relationId,
                       WorkerSharedMem *worker,
                       char *key,
                       size_t keyLen,
                       char **val,
                       size_t *valLen);

extern void PutRequest(Oid relationId,
                       WorkerSharedMem *worker,
                       char *key,
                       size_t keyLen,
                       char *val,
                       size_t valLen);

extern void DeleteRequest(Oid relationId,
                          WorkerSharedMem *worker,
                          char *key,
                          size_t keyLen);

extern RingBufSharedMem* BeginLoadRequest(Oid relationId,
                                          WorkerSharedMem *worker);

extern void LoadTuple(RingBufSharedMem *buf,
                      char *key,
                      size_t keyLen,
                      char *val,
                      size_t valLen);

extern uint64 EndLoadRequest(Oid relationId,
                             WorkerSharedMem *worker,
                             RingBufSharedMem* buf);

#ifdef VIDARDB
extern bool RangeQueryRequest(Oid relationId,
                              uint64 operationId,
                              WorkerSharedMem *worker,
                              RangeQueryOptions *options,
                              char **buf,
                              size_t *bufLen);

extern void ClearRangeQueryMetaRequest(Oid relationId,
                                       uint64 operationId,
                                       WorkerSharedMem *worker,
                                       TableReadState *readState);
#endif

extern void TerminateRequest(WorkerProcKey *workerKey,
                             ManagerSharedMem **managerPtr);

/*
 * Utility for worker hash table
 */
extern int CompareWorkerProcKey(const void *key1,
                                const void *key2,
                                Size keysize);

#endif
