
#ifdef VIDARDB
#include "vidardb/db.h"
#include "vidardb/options.h"
#include "vidardb/table.h"
#include "vidardb/splitter.h"
#include "vidardb/comparator.h"
using namespace vidardb;
#include <list>
#include <algorithm>
#include <mutex>
#include <thread>
#include <iostream>
#else
#include "rocksdb/db.h"
#include "rocksdb/options.h"
using namespace rocksdb;
#endif
using namespace std;

#include "kv_storage.h"


extern "C" {

#include "kv_fdw.h"
#include "fmgr.h"
#include "access/xact.h"
#include "access/tupmacs.h"
#include "utils/typcache.h"
#include "miscadmin.h"


#ifdef VIDARDB
void* Open(char* path, bool useColumn, int attrCount, ComparatorOptions* opts) {
    DB* db = nullptr;
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

    Status s = DB::Open(options, string(path), &db);
    assert(s.ok());
    return db;
}
#else
void* Open(char* path) {
    DB* db = nullptr;
    Options options;
    options.create_if_missing = true;

    Status s = DB::Open(options, string(path), &db);
    assert(s.ok());
    return db;
}
#endif

void Close(void* db) {
    DB* db_ptr = static_cast<DB*>(db);
    Options opts = db_ptr->GetOptions();
    const Comparator* wrap_cmp = opts.comparator;
    const Comparator* root_cmp = wrap_cmp->GetRootComparator();
    delete root_cmp;
    delete db_ptr;
}

uint64 Count(void* db) {
    string count;
    #ifdef VIDARDB
    static_cast<DB*>(db)->GetProperty("vidardb.estimate-num-keys", &count);
    #else
    static_cast<DB*>(db)->GetProperty("rocksdb.estimate-num-keys", &count);
    #endif
    return stoull(count);
}

void* GetIter(void* db) {
    Iterator* it = static_cast<DB*>(db)->NewIterator(ReadOptions());
    it->SeekToFirst();
    return it;
}

void DelIter(void* it) {
    delete static_cast<Iterator*>(it);
}

bool Next(void* db, void* iter, char* buffer) {
    Iterator* it = static_cast<Iterator*>(iter);
    if (it == nullptr || !it->Valid()) return false;

    size_t keyLen = it->key().size(), valLen = it->value().size();
    memcpy(buffer, &keyLen, sizeof(keyLen));
    buffer += sizeof(keyLen);
    memcpy(buffer, it->key().data(), keyLen);
    buffer += keyLen;
    memcpy(buffer, &valLen, sizeof(valLen));
    buffer += sizeof(valLen);
    memcpy(buffer, it->value().data(), valLen);

    it->Next();
    return true;
}

bool ReadBatch(void* db, void* iter, char* buf, size_t* bufLen) {
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

bool Get(void* db, char* key, size_t keyLen, char** val, size_t* valLen) {
    string sval;
    ReadOptions ro;
    Status s = static_cast<DB*>(db)->Get(ro, Slice(key, keyLen), &sval);
    if (!s.ok()) return false;
    *valLen = sval.length();
    *val = static_cast<char*>(palloc(*valLen));
    memcpy(*val, sval.c_str(), *valLen);
    return true;
}

bool Put(void* db, char* key, size_t keyLen, char* val, size_t valLen) {
    Status s = static_cast<DB*>(db)->Put(WriteOptions(), Slice(key, keyLen),
                                         Slice(val, valLen));
    return s.ok();
}

bool Delete(void* db, char* key, size_t keyLen) {
    Status s = static_cast<DB*>(db)->Delete(WriteOptions(), Slice(key, keyLen));
    return s.ok();
}

/* copied from the storage engine */
inline char* EncodeVarint64(char* dst, uint64_t v) {
    static const unsigned int B = 128;
    unsigned char* ptr = reinterpret_cast<unsigned char*>(dst);
    while (v >= B) {
        *(ptr++) = (v & (B - 1)) | B;
        v >>= 7;
    }
    *(ptr++) = static_cast<unsigned char>(v);
    return reinterpret_cast<char*>(ptr);
}

uint8 EncodeVarintLength(uint64 len, char* buf) {
    char* ptr = EncodeVarint64(buf, len);
    return (ptr - buf);
}

/* copied from the storage engine */
inline const char* GetVarint64Ptr(const char* p, const char* limit,
                                  uint64_t* value) {
    uint64_t result = 0;
    for (uint32_t shift = 0; shift <= 63 && p < limit; shift += 7) {
        uint64_t byte = *(reinterpret_cast<const unsigned char*>(p));
        p++;
        if (byte & 128) {
            // More bytes are present
            result |= ((byte & 127) << shift);
        } else {
            result |= (byte << shift);
            *value = result;
            return reinterpret_cast<const char*>(p);
        }
    }
    return nullptr;
}

uint8 DecodeVarintLength(char* start, char* limit, uint64* len) {
    const char* ret = GetVarint64Ptr(start, limit, len);
    return ret ? (ret - start) : 0;
}

#ifdef VIDARDB
void ParseRangeQueryOptions(RangeQueryOptions* queryOptions, void** range,
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

bool RangeQuery(void* db, void* range, void** readOptions, size_t* bufLen,
                void** result) {
    ReadOptions* ro = static_cast<ReadOptions*>(*readOptions);
    Range* r = static_cast<Range*>(range);
    list<RangeQueryKeyVal>* res = static_cast<list<RangeQueryKeyVal>*>(*result);
    if (res == NULL) {
        res = new list<RangeQueryKeyVal>;
    }
    Status s;
    bool ret = static_cast<DB*>(db)->RangeQuery(*ro, *r, *res, &s);

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
        *bufLen = ro->result_key_size +
                  ro->result_val_size +
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

/* Comparator wrapper for PG datatype's comparison function */
class PGDataTypeComparator : public Comparator {
  public:
    PGDataTypeComparator(ComparatorOptions* options) : options_(*options) {
        if (OidIsValid(options_.cmpFuncOid)) {
            fmgr_info(options_.cmpFuncOid, &fmgr_info_);
        }
        name_map_mutex_ = new std::mutex;
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
        return KVDATATYPECOMPARATOR;
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

        std::cout<<std::this_thread::get_id()<<std::endl;

        /* fetch attribute value */
        Datum arg1 = fetch_att(a.data(), options_.attrByVal, options_.attrLength);
        Datum arg2 = fetch_att(b.data(), options_.attrByVal, options_.attrLength);


        name_map_mutex_->lock();
        set_stack_base();
        /* generally, the return type is int4 (pg_proc.dat) */
        StartTransactionCommand();  /* necessary transaction */
//        TypeCacheEntry *typentry = lookup_type_cache(23, TYPECACHE_CMP_PROC_FINFO);
        int ret = DatumGetInt32(FunctionCall2Coll((FmgrInfo*) &fmgr_info_,
                                                  options_.attrCollOid,
                                                  arg1, arg2));
        CommitTransactionCommand();  /* necessary transaction */
        name_map_mutex_->unlock();

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
    ComparatorOptions options_;
    FmgrInfo fmgr_info_;
    std::mutex *name_map_mutex_;
};

void* NewDataTypeComparator(ComparatorOptions* options) {
    return new PGDataTypeComparator(options);
}

#endif

}
