/* Copyright 2019-present VidarDB Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef KV_STORAGE_H_
#define KV_STORAGE_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "../kv_api.h"


#define READBATCHSIZE 4096*20


/**
 * C wrapper of storage engine API
 */

#ifdef VIDARDB
void*  OpenConn(char* path, bool useColumn, int attrCount, ComparatorOpts* opts);
#else
void*  OpenConn(char* path, ComparatorOpts* opts);
#endif
void   CloseConn(void* conn);
uint64 GetCount(void* conn);
void*  GetIter(void* conn);
void   DelIter(void* it);
bool   BatchRead(void* conn, void* iter, char* buf, size_t* bufLen);
bool   GetRecord(void* conn, char* key, size_t keyLen, char** val, size_t* valLen);
bool   PutRecord(void* conn, char* key, size_t keyLen, char* val, size_t valLen);
bool   DelRecord(void* conn, char* key, size_t keyLen);
#ifdef VIDARDB
void   ParseRangeQueryOptions(RangeQueryOpts* queryOptions, void** range,
                              void** readOptions);
bool   RangeQueryRead(void* conn, void* range, void** readOptions,
                      size_t* bufLen, void** result);
void   ParseRangeQueryResult(void* result, char* buf);
void   ClearRangeQueryMeta(void* range, void* readOptions);
#endif

#ifdef __cplusplus
}
#endif

#endif  /* KV_STORAGE_H_ */
