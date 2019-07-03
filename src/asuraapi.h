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
typedef struct {
    void *db;                              /**< dummy member */
} KVDB;

typedef struct {
    void *cur;                             /**< dummy member */
} CUR;

KVDB *dbnew();
void dbdel(KVDB *db);
bool dbopen(KVDB *db, const char *host, int32_t port, double timeout);
bool dbclose(KVDB *db);

int64_t dbcount(KVDB *db);

CUR *getcur(KVDB *DB);
void delcur(CUR *cur);
bool next(KVDB *db, CUR *cur, char **key, char **value);
bool get(KVDB *db, char *key, char **value);

bool add(KVDB *db, const char *key, const char *value);
bool replace(KVDB *db, const char *key, const char *value);
bool remove(KVDB *db, const char *key);

const char *geterror(KVDB* db);
const char *geterrormsg(KVDB* db);

#if defined(__cplusplus)
}
#endif

#endif                                   /* duplication check */
