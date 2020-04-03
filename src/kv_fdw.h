
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
#define KVKEYJUNK "__key_junk"

#define KVFDWNAME "kv_fdw"

#define BACKFILE "/KVSharedMem"

#define PERMISSION 0777

#define PATHMAXLENGTH 4096

#define BUFSIZE 65536 * sizeof(char)

#define FILENAMELENGTH 20

#define RESPONSEFILE "/KVSharedResponse"

#define RESPONSEQUEUELENGTH 2

/* Defines for valid options and the default values */
#define OPTION_FILENAME "filename"

#ifdef VIDARDB
#define OPTION_STORAGE_FORMAT "storage"

#define OPTION_BATCH_CAPACITY "batch"

#define COLUMNSTORE "column"

#define BATCHCAPACITY 10000
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
    char area[BUFSIZE];  // assume ~64K for a tuple is enough
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
    int attrCount;
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
    bool done;
    StringInfo key;

    #ifdef VIDARDB
    bool useColumn;
    char *buf;      // shared mem for data returned by RangeQuery
    size_t bufLen;  // shared mem length
    char *next;     // pointer to the next data entry for IterateForeignScan
    bool hasNext;   // whether there will be a next batch from RangeQuery
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
    AttrNumber keyJunkNo;

    #ifdef VIDARDB
    bool useColumn;
    #endif
} TableWriteState;

typedef enum FuncName {
    OPEN = 0,
    CLOSE,
    COUNT,
    GETITER,
    DELITER,
    NEXT,
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

extern void SerializeAttribute(TupleDesc tupleDescriptor,
                               Index index,
                               Datum datum,
                               StringInfo buffer,
                               bool useDelimiter);

extern char *KVGetOptionValue(Oid foreignTableId, const char *optionName);

extern Datum ShortVarlena(Datum datum, int typeLength, char storage);

extern void *KVStorageThreadFun(void *arg);

extern SharedMem *OpenRequest(Oid relationId, SharedMem *ptr, ...);

extern void CloseRequest(Oid relationId, SharedMem *ptr);

extern uint64 CountRequest(Oid relationId, SharedMem *ptr);

extern void GetIterRequest(Oid relationId, SharedMem *ptr);

extern void DelIterRequest(Oid relationId, SharedMem *ptr);

extern bool NextRequest(Oid relationId,
                        SharedMem *ptr,
                        char **key,
                        size_t *keyLen,
                        char **val,
                        size_t *valLen);

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
                              SharedMem *ptr,
                              RangeQueryOptions *options,
                              char **buf,
                              size_t *bufLen);

extern void ClearRangeQueryMetaRequest(Oid relationId, SharedMem *ptr);
#endif

/* global variables */

pthread_t kvStorageThread;


#endif
