/*
 * This is a C wrapper around C++ interface.
 * Needed for PostgreSQL integration
 *
 */


#ifndef _KVAPI_H                       /* duplication check */
#define _KVAPI_H

#if defined(__cplusplus)
extern "C" {
#endif

#if !defined(__STDC_LIMIT_MACROS)
#define __STDC_LIMIT_MACROS  1           /**< enable limit macros for C++ */
#endif

#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <float.h>
#include <limits.h>
#include <locale.h>
#include <math.h>
#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <string.h>
#include <time.h>
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
bool Next(void* db, void* it, char** key, char** value);

bool Get(void* db, char* key, char** value);
bool Put(void* db, char* key, char* value);
bool Delete(void* db, char* key);


#if defined(__cplusplus)
}
#endif

#endif                                   /* duplication check */
