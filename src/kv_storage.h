
#ifndef _KV_H
#define _KV_H

#if defined(__cplusplus)
extern "C" {
#endif

#include <stdint.h>
#include <stdbool.h>
#include <fcntl.h>
#include "postgres.h"
#include "kv_posix.h"

/**
 * C wrapper
 */

#ifdef VidarDB
void* Open(char* path, bool useColumn, int ColumnNumber);
#else
void* Open(char* path);
#endif
void Close(void* db);

uint64 Count(void* db);

void* GetIter(void* db);
void DelIter(void* it);
bool Next(void* db, void* iter, char** key, size_t* keyLen,
          char** val, size_t* valLen);

bool Get(void* db, char* key, size_t keyLen, char** val, size_t* valLen);
bool Put(void* db, char* key, size_t keyLen, char* val, size_t valLen);
bool Delete(void* db, char* key, size_t keyLen);

#ifdef VidarDB
#define BATCHCAPACITY 10000
#define FILENAMELENGTH 20
#define PERMISSION 0777
#define RANGEQUERYFILE "/KVRangeQuery"

typedef struct RangeQueryOptions {
    size_t startLen;
    char* start;
    size_t limitLen;
    char* limit;
    size_t targetNum;
    uint32_t* targetIndexes;
    int32 batchCapacity;
} RangeQueryOptions;

bool RangeQuery(void* db, void** readOptions, RangeQueryOptions* rangeQueryOptions, pid_t pid, size_t* buffSize);
#endif

#if defined(__cplusplus)
}
#endif

#endif
