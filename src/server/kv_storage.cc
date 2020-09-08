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

#ifdef VIDARDB
#include <list>
#include <algorithm>
#include "vidardb/db.h"
#include "vidardb/options.h"
#include "vidardb/table.h"
#include "vidardb/splitter.h"
#include "vidardb/comparator.h"
using namespace vidardb;
#else
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/comparator.h"
using namespace rocksdb;
#endif

#include <mutex>
using namespace std;

#include "kv_storage.h"


extern "C" {
#include "fmgr.h"
#include "access/xact.h"
#include "access/tupmacs.h"
#include "miscadmin.h"
#include "utils/resowner.h"
}

/*
 * Forward Declaration
 * Create a datatype comparator wrapper for storage engine
 */
void* NewDataTypeComparator(ComparatorOpts* options);


#ifdef VIDARDB
void* OpenConn(char* path, bool useColumn, int attrCount, ComparatorOpts* opts) {
    DB* conn = nullptr;
    Options options;
    options.OptimizeAdaptiveLevelStyleCompaction();
    options.create_if_missing = true;
    options.comparator = static_cast<Comparator*>(NewDataTypeComparator(opts));

    shared_ptr<TableFactory> block_based_table(NewBlockBasedTableFactory());
    shared_ptr<TableFactory> column_table(NewColumnTableFactory());
    ColumnTableOptions* column_opts =
        static_cast<ColumnTableOptions*>(column_table->GetOptions());
    /* TODO: currently, we assume 1 attribute primary key */
    column_opts->column_count = attrCount - 1;

    options.splitter.reset(NewEncodingSplitter());
    options.table_factory.reset(NewAdaptiveTableFactory(block_based_table,
        block_based_table, column_table, useColumn? 0: -1));

    Status s = DB::Open(options, string(path), &conn);
    if (!s.ok()) {
        ereport(ERROR, (errmsg("DB open status: %s", s.ToString().c_str())));
    }
    return conn;
}
#else
void* OpenConn(char* path, ComparatorOpts* opts) {
    DB* conn = nullptr;
    Options options;
    options.create_if_missing = true;
    options.comparator = static_cast<Comparator*>(NewDataTypeComparator(opts));

    Status s = DB::Open(options, string(path), &conn);
    if (!s.ok()) {
        ereport(ERROR, (errmsg("DB open status: %s", s.ToString().c_str())));
    }
    return conn;
}
#endif

void CloseConn(void* conn) {
    DB* db_ptr = static_cast<DB*>(conn);
    const Comparator* wrap_cmp = db_ptr->GetOptions().comparator;
    const Comparator* root_cmp = wrap_cmp->GetRootComparator();
    delete root_cmp;
    delete db_ptr;
}

uint64 GetCount(void* conn) {
    string count;
    #ifdef VIDARDB
    static_cast<DB*>(conn)->GetProperty("vidardb.estimate-num-keys", &count);
    #else
    static_cast<DB*>(conn)->GetProperty("rocksdb.estimate-num-keys", &count);
    #endif
    return stoull(count);
}

void* GetIter(void* conn) {
    Iterator* it = static_cast<DB*>(conn)->NewIterator(ReadOptions());
    it->SeekToFirst();
    return it;
}

void DelIter(void* it) {
    delete static_cast<Iterator*>(it);
}

bool BatchRead(void* conn, void* iter, char* buf, size_t* bufLen) {
    *bufLen = 0;

    Iterator* it = static_cast<Iterator*>(iter);
    while (it != nullptr && it->Valid()) {
        size_t keyLen = it->key().size(), valLen = it->value().size();
        *bufLen += keyLen + valLen + sizeof(keyLen) + sizeof(valLen);

        if (*bufLen > READBATCHSIZE) {
            *bufLen -= keyLen + valLen + sizeof(keyLen) + sizeof(valLen);
            break;
        }

        memcpy(buf, &keyLen, sizeof(keyLen));
        buf += sizeof(keyLen);
        memcpy(buf, it->key().data(), keyLen);
        buf += keyLen;
        memcpy(buf, &valLen, sizeof(valLen));
        buf += sizeof(valLen);
        memcpy(buf, it->value().data(), valLen);
        buf += valLen;

        it->Next();
    }

    /* does not have next */
    if (it == nullptr || !it->Valid()) {
        return false;
    }

    return true;
}

bool GetRecord(void* conn, char* key, size_t keyLen, char** val, size_t* valLen) {
    string sval;
    ReadOptions ro;
    Status s = static_cast<DB*>(conn)->Get(ro, Slice(key, keyLen), &sval);
    if (!s.ok()) return false;
    *valLen = sval.length();
    *val = static_cast<char*>(palloc(*valLen));
    memcpy(*val, sval.c_str(), *valLen);
    return true;
}

bool PutRecord(void* conn, char* key, size_t keyLen, char* val, size_t valLen) {
    Status s = static_cast<DB*>(conn)->Put(WriteOptions(), Slice(key, keyLen),
                                           Slice(val, valLen));
    return s.ok();
}

bool DelRecord(void* conn, char* key, size_t keyLen) {
    Status s = static_cast<DB*>(conn)->Delete(WriteOptions(), Slice(key, keyLen));
    return s.ok();
}

#ifdef VIDARDB
void ParseRangeQueryOptions(RangeQueryOpts* queryOptions, void** range,
                            void** readOptions) {
    Assert(queryOptions != NULL);

    /* Parse Range */
    Range* r = static_cast<Range*>(*range);
    if (*range == NULL) {
        r = static_cast<Range*>(palloc0(sizeof(Range)));
    }
    r->start = queryOptions->startLen > 0 ?
               Slice(queryOptions->start, queryOptions->startLen) : kRangeQueryMin;
    r->limit = queryOptions->limitLen > 0 ?
               Slice(queryOptions->limit, queryOptions->limitLen) : kRangeQueryMax;
    *range = r;

    /* Parse ReadOptions */
    ReadOptions* options = static_cast<ReadOptions*>(*readOptions);
    if (options == NULL) {
        /* since there is vector in ReadOptions, so we use 'new' here */
        options = new ReadOptions();
    }

    for (int i = 0; i < queryOptions->attrCount; i++) {
        AttrNumber attr = *(queryOptions->attrs + i);
        options->columns.push_back(attr);
    }

    sort(options->columns.begin(), options->columns.end());
    printf("\nattrs in %s: ", __func__);
    for (auto i : options->columns) printf(" %d ", i);
    printf("\n");
    options->batch_capacity = queryOptions->batchCapacity;

    *readOptions = options;
}

bool RangeQueryRead(void* conn, void* range, void** readOptions, size_t* bufLen,
                    void** result) {
    ReadOptions* ro = static_cast<ReadOptions*>(*readOptions);
    Range* r = static_cast<Range*>(range);
    list<RangeQueryKeyVal>* res = static_cast<list<RangeQueryKeyVal>*>(*result);
    if (res == NULL) {
        res = new list<RangeQueryKeyVal>;
    }
    Status s;
    bool ret = static_cast<DB*>(conn)->RangeQuery(*ro, *r, *res, &s);

    if (!s.ok()) {
        *result = res;
        *bufLen = 0;
        return false;
    }
    if (res->size() == 0) {
        /*
         * low possibility with only deleted keys in this batch,
         * but potentially remains more batches
         */
        *bufLen = 0;
    } else {
        *bufLen = ro->result_key_size + ro->result_val_size +
                  sizeof(size_t) * 2 * res->size();
    }

    *result = res;
    return ret;
}

void ParseRangeQueryResult(void* result, char* buf) {
    Assert(result != NULL);
    list<RangeQueryKeyVal>* res = static_cast<list<RangeQueryKeyVal>*>(result);

    if (buf != NULL) {
        for (auto it = res->begin(); it != res->end(); ++it) {
            size_t keyLen = it->user_key.size();
            memcpy(buf, &keyLen, sizeof(keyLen));
            buf += sizeof(keyLen);
            memcpy(buf, it->user_key.c_str(), keyLen);
            buf += keyLen;

            size_t valLen = it->user_val.size();
            memcpy(buf, &valLen, sizeof(valLen));
            buf += sizeof(valLen);
            memcpy(buf, it->user_val.c_str(), valLen);
            buf += valLen;
        }
    }

    delete res;
}

void ClearRangeQueryMeta(void* range, void* readOptions) {
    Range* r = static_cast<Range*>(range);
    if (r->start != kRangeQueryMin) {
        pfree(const_cast<void*>(static_cast<const void*>(r->start.data())));
    }
    if (r->limit != kRangeQueryMax) {
        pfree(const_cast<void*>(static_cast<const void*>(r->limit.data())));
    }
    pfree(range);

    ReadOptions* options = static_cast<ReadOptions*>(readOptions);
    delete options;
}
#endif

/* Comparator wrapper for PG datatype's comparison function */
class PGDataTypeComparator : public Comparator {
  public:
    PGDataTypeComparator(ComparatorOpts* options) : options_(*options) {
        if (!OidIsValid(options_.cmpFuncOid)) {
            return;
        }
        fmgr_info(options_.cmpFuncOid, &funcManager_);
        mutex_ = new std::mutex;
        firstCall_ = new bool;
        *firstCall_ = true;
        resourceOwner_ = ResourceOwnerCreate(NULL, "ComparatorResourceOwner");
        funcCallInfo_ = (FunctionCallInfoBaseData*) palloc0(sizeof(*funcCallInfo_));
        InitFunctionCallInfoData(*funcCallInfo_, &funcManager_, 2,
                                 options_.attrCollOid, NULL, NULL);
        funcCallInfo_->args[0].isnull = false;
        funcCallInfo_->args[1].isnull = false;
    }

    virtual ~PGDataTypeComparator() {
        if (!OidIsValid(options_.cmpFuncOid)) {
            return;
        }
        pfree(funcCallInfo_);
        ResourceOwnerDelete(resourceOwner_);
        delete firstCall_;
        delete mutex_;
    }

    /*
     * The name of the comparator.  Used to check for comparator
     * mismatches (i.e., a DB created with one comparator is
     * accessed using a different comparator.
     *
     * The client of this package should switch to a new name whenever
     * the comparator implementation changes in a way that will cause
     * the relative ordering of any two keys to change.
     */
    virtual const char* Name() const override {
        return "kv.PGDataTypeComparator";
    }

    /*
     * Three-way comparison.  Returns value:
     *   < 0 iff "a" < "b",
     *   == 0 iff "a" == "b",
     *   > 0 iff "a" > "b"
     */
    virtual int Compare(const Slice& a, const Slice& b) const override {
        /* datatype comparison func does not exist */
        if (!OidIsValid(options_.cmpFuncOid)) {
            return a.compare(b);
        }

        /* fetch attribute value */
        Datum arg1 = fetch_att(a.data(), options_.attrByVal, options_.attrLength);
        Datum arg2 = fetch_att(b.data(), options_.attrByVal, options_.attrLength);
        int ret = 0;

        /* contention area */
        mutex_->lock();

        funcCallInfo_->args[0].value = arg1;
        funcCallInfo_->args[1].value = arg2;

        if (*firstCall_ == true) {
            /*
             * must start a transaction to build the type cache, and pass
             * through system cache check, but it is fine, just for one time.
             * TODO: We have to be careful about cache invalidation problem.
             * currently, we assume tbl schema never get changed after creation.
             */
            StartTransactionCommand();  /* necessary transaction */
            /* generally, the return type is int4 (pg_proc.dat) */
            ret = DatumGetInt32(FunctionCallInvoke(funcCallInfo_));
            CommitTransactionCommand();  /* necessary transaction */
            *firstCall_ = false;
        } else {
            /*
             * set CurrentResourceOwner and stack_base to pass checks
             */
            ResourceOwner old_owner = CurrentResourceOwner;
            CurrentResourceOwner = resourceOwner_;
            pg_stack_base_t old = set_stack_base();

            ret = DatumGetInt32(FunctionCallInvoke(funcCallInfo_));

            restore_stack_base(old);
            CurrentResourceOwner = old_owner;
        }

        mutex_->unlock();

        return ret;
    }

    /*
     * Compares two slices for equality. The following invariant should always
     * hold (and is the default implementation):
     *   Equal(a, b) iff Compare(a, b) == 0
     * Overwrite only if equality comparisons can be done more efficiently than
     * three-way comparisons.
     */
    virtual bool Equal(const Slice& a, const Slice& b) const override {
        return Compare(a, b) == 0;
    }

    /*
     * Advanced functions: these are used to reduce the space requirements
     * for internal data structures like index blocks.
     *
     * If *start < limit, changes *start to a short string in [start,limit).
     * Simple comparator implementations may return with *start unchanged,
     * i.e., an implementation of this method that does nothing is correct.
     */
    virtual void FindShortestSeparator(std::string* start,
                                       const Slice& limit) const override {
        /* do nothing */
    }

    /*
     * Changes *key to a short string >= *key.
     * Simple comparator implementations may return with *key unchanged,
     * i.e., an implementation of this method that does nothing is correct.
     */
    virtual void FindShortSuccessor(std::string* key) const override {
        /* do nothing */
    }

  private:
    ComparatorOpts options_;
    FmgrInfo funcManager_;
    std::mutex* mutex_;
    bool* firstCall_;
    ResourceOwner resourceOwner_;
    FunctionCallInfoBaseData* funcCallInfo_;
};

void* NewDataTypeComparator(ComparatorOpts* options) {
    return new PGDataTypeComparator(options);
}
