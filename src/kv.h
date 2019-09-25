
#ifndef _KV_H
#define _KV_H

#if defined(__cplusplus)
extern "C" {
#endif

#include <stdint.h>
#include <stdbool.h>

/**
 * C wrapper
 */

void* Open(char* path);
void Close(void* db);

uint64_t Count(void* db);

void* GetIter(void* db);
void DelIter(void* it);
bool Next(void* db, void* iter, char** key, unsigned int *keyLen,
          char** value, unsigned int *valLen);

bool Get(void* db, char* key, char** value);
bool Put(void* db, char* key, unsigned int keyLen,
         char* value, unsigned int valLen);
bool Delete(void* db, char* key, unsigned int keyLen);


#if defined(__cplusplus)
}
#endif

#endif
