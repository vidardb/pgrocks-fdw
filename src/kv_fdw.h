
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


/* Defines */
#define KVFDWNAME "kv_fdw"

#define BACKFILE "/KVSharedMem"

#define PERMISSION 0777

#define PATHMAXLENGTH 4096

#define FILENAMELENGTH 25

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


/* Shared memory for function requests: 
 * mutex: mutual exclusion of the request buffer;
 * full: tell whether the request buffer is full;
 * agent[2]: synchronize the creation of the worker process;
 * worker: notify the worker process after a request is submitted;
 * responseMutexes[RESPONSEQUEUELENGTH]:
 *     mutual exclusion of the response buffer;
 * responseSync[RESPONSEQUEUELENGTH]:
 *     notify child processes after the response is ready.
 */
typedef struct SharedMem {
    sem_t mutex;
    sem_t full;
    sem_t agent[2];
    sem_t worker;
    sem_t responseMutex[RESPONSEQUEUELENGTH];
    sem_t responseSync[RESPONSEQUEUELENGTH];
    bool workerProcessCreated;
    char area[BUFSIZE];  /* assume ~64K for a tuple is enough */
} SharedMem;

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
    TERMINATE
} FuncName;


/* Function declarations for extension loading and unloading */
extern void _PG_init(void);

extern void _PG_fini(void);


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

extern void *KVStorageThreadFun(void *arg);

extern SharedMem *OpenRequest(Oid relationId, SharedMem *ptr, ...);

extern void CloseRequest(Oid relationId, SharedMem *ptr);

extern uint64 CountRequest(Oid relationId, SharedMem *ptr);

extern void GetIterRequest(Oid relationId, uint64 operationId, SharedMem *ptr);

extern void DelIterRequest(Oid relationId,
                           uint64 operationId,
                           SharedMem *ptr,
                           TableReadState *readState);

extern bool NextRequest(Oid relationId,
                        uint64 operationId,
                        SharedMem *ptr,
                        char **key,
                        size_t *keyLen,
                        char **val,
                        size_t *valLen);

extern bool ReadBatchRequest(Oid relationId,
                             uint64 operationId,
                             SharedMem *ptr,
                             char **buf,
                             size_t *bufLen);

extern bool GetRequest(Oid relationId,
                       SharedMem *ptr,
                       char *key,
                       size_t keyLen,
                       char **val,
                       size_t *valLen);

extern void PutRequest(Oid relationId,
                       SharedMem *ptr,
                       char *key,
                       size_t keyLen,
                       char *val,
                       size_t valLen);

extern void DeleteRequest(Oid relationId,
                          SharedMem *ptr,
                          char *key,
                          size_t keyLen);

#ifdef VIDARDB
extern bool RangeQueryRequest(Oid relationId,
                              uint64 operationId,
                              SharedMem *ptr,
                              RangeQueryOptions *options,
                              char **buf,
                              size_t *bufLen);

extern void ClearRangeQueryMetaRequest(Oid relationId,
                                       uint64 operationId,
                                       SharedMem *ptr,
                                       TableReadState *readState);
#endif


#endif
