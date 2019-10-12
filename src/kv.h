
#ifndef _KV_H
#define _KV_H

#if defined(__cplusplus)
extern "C" {
#endif

#include <stdint.h>
#include <stdbool.h>
#include "postgres.h"

/**
 * C wrapper
 */

void* Open(char* path);
void Close(void* db);

uint64 Count(void* db);

void* GetIter(void* db);
void DelIter(void* it);
bool Next(void* db, void* iter, char** key, uint32* keyLen,
          char** value, uint32* valLen);

bool Get(void* db, char* key, uint32 keyLen, char** value, uint32* valLen);
bool Put(void* db, char* key, uint32 keyLen, char* value, uint32 valLen);
bool Delete(void* db, char* key, uint32 keyLen);


#if defined(__cplusplus)
}
#endif

#endif
