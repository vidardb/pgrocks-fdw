
#ifdef VIDARDB
#include "vidardb/db.h"
#include "vidardb/options.h"
#include "vidardb/table.h"
#include "vidardb/splitter.h"
using namespace vidardb;
#include <list>
#include <algorithm>
#else
#include "rocksdb/db.h"
#include "rocksdb/options.h"
using namespace rocksdb;
#endif
using namespace std;

#include "kv_storage.h"

extern "C" {

#ifdef VIDARDB
void* Open(char* path, bool useColumn, int attrCount) {
    DB* db = nullptr;
    Options options;
    options.OptimizeAdaptiveLevelStyleCompaction();
    options.create_if_missing = true;

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
    delete static_cast<DB*>(db);
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

bool Next(void* db, void* iter, char** key, size_t* keyLen, char** val,
          size_t* valLen) {
    Iterator* it = static_cast<Iterator*>(iter);
    if (it == nullptr || !it->Valid()) return false;

    *keyLen = it->key().size(), *valLen = it->value().size();
    *key = static_cast<char*>(palloc(*keyLen));
    *val = static_cast<char*>(palloc(*valLen));

    memcpy(*key, it->key().data(), *keyLen);
    memcpy(*val, it->value().data(), *valLen);

    it->Next();
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
        options = static_cast<ReadOptions*>(palloc0(sizeof(ReadOptions)));
    }

    for (int i = 0; i < queryOptions->attrCount; i++) {
        AttrNumber attr = *(queryOptions->attrs + i);
        options->columns.push_back(attr);
    }

    sort(options->columns.begin(), options->columns.end());
    printf("\nattrs: ");
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
    pfree(readOptions);
}

#endif

}
