/*
 * This is a C wrapper around Asura's C++ interface.
 * Needed for PostgreSQL integration
 *
 */


#ifndef _ASURAAPI_H                       /* duplication check */
#define _ASURAAPI_H

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

/**
 * C wrapper
 */

void* dbopen();
void dbclose(void* db);

uint64_t dbcount(void* db);

void* getiter(void* db);
void delcur(void* it);
bool next(void* db, void* it, char** key, char** value);

bool get(void* db, char* key, char** value);
bool add(void* db, char* key, char* value);
bool remove(void* db, char* key);


#if defined(__cplusplus)
}
#endif

#endif                                   /* duplication check */
