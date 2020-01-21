
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

void* Open(char* path);
void Close(void* db);

uint64 Count(void* db);

void* GetIter(void* db);
void DelIter(void* it);
bool Next(void* db, void* iter, char** key, uint32* keyLen,
          char** val, uint32* valLen);

bool Get(void* db, char* key, uint32 keyLen, char** val, uint32* valLen);
bool Put(void* db, char* key, uint32 keyLen, char* val, uint32 valLen);
bool Delete(void* db, char* key, uint32 keyLen);

#ifdef VidarDB
#define BATCHCAPACITY 10000
#define FILENAMELENGTH 20
#define PERMISSION 0777
#define RANGEQUERYFILE "/KVRangeQuery"

typedef struct RangeSpec {
    char* start;
    uint32 startLen;
    char* limit;
    uint32 limitLen;
} RangeSpec;

bool RangeQuery(void* db, void** readOptions, RangeSpec range, pid_t pid, size_t* buffSize);
#endif

#if defined(__cplusplus)
}
#endif

#endif
