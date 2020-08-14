/* Copyright 2020 VidarDB Inc.
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

/*
 * Implementation for kv connection
 */

#include "kv_db.h"
#include "kv_storage.h" /* deprecated */

#ifdef VIDARDB
void*
KVConnection::open(char* path, ComparatorOpt* opt, bool useColumn, int attrCount)
{
    if (db)
    {
        ref++;
        return db;
    }

    ComparatorOptions option;
    option.cmpFuncOid = opt->cmpFuncOid;
    option.attrCollOid = opt->attrCollOid;
    option.attrByVal = opt->attrByVal;
    option.attrLength = opt->attrLength;
    db = Open(path, useColumn, attrCount, &option);
    return db;
}
#else
void*
KVConnection::open(char* path, ComparatorOpt* opt)
{
    if (db)
    {
        ref++;
        return db;
    }

    ComparatorOptions option;
    option.cmpFuncOid = opt->cmpFuncOid;
    option.attrCollOid = opt->attrCollOid;
    option.attrByVal = opt->attrByVal;
    option.attrLength = opt->attrLength;
    db = Open(path, &option);
    return db;
}
#endif

void
KVConnection::close(void* db)
{

}

uint64
KVConnection::count(void* db)
{
    return 0;
}
