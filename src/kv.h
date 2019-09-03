
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

void* Open();
void Close(void* db);

uint64_t Count(void* db);

void* GetIter(void* db);
void DelIter(void* it);
bool Next(void* db, void* iter, char** key, char** value);

bool Get(void* db, char* key, char** value);
bool Put(void* db, char* key, char* value);
bool Delete(void* db, char* key);


#if defined(__cplusplus)
}
#endif

#endif
