
#ifdef VIDARDB
#include "vidardb/db.h"
#include "vidardb/options.h"
#include "vidardb/table.h"
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
    column_opts->splitter.reset(new PipeSplitter());
    /* TODO: currently, we assume 1 attribute primary key */
    column_opts->column_count = attrCount - 1;

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
    Status s = static_cast<DB*>(db)->Get(ReadOptions(), Slice(key, keyLen), &sval);
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

#ifdef VIDARDB
void ParseRangeQueryOptions(RangeQueryOptions* queryOptions, void** range,
                            void** readOptions) {
    /* Parse Range */
    Range* r = static_cast<Range*>(*range);
    if (*range == NULL) {
        r = static_cast<Range*>(palloc0(sizeof(Range)));
    }
    if (queryOptions != NULL) {
        if (queryOptions->startLen > 0) {
            r->start = Slice(queryOptions->start, queryOptions->startLen);
        } else {
            r->start = kRangeQueryMin;
        }
        if (queryOptions->limitLen > 0) {
            r->limit = Slice(queryOptions->limit, queryOptions->limitLen);
        } else {
            r->limit = kRangeQueryMax;
        }
    }
    *range = r;

    /* Parse ReadOptions */
    ReadOptions* options = static_cast<ReadOptions*>(*readOptions);
    if (options == NULL) {
        options = static_cast<ReadOptions*>(palloc0(sizeof(ReadOptions)));
    }

    /* TODO: currently, first attribute in the value must be returned */   
    bool containsOne = false;
    if (queryOptions != NULL) {
        for (int i = 0; i < queryOptions->attrCount; i++) {
            AttrNumber attr = *(queryOptions->attrs + i);
            options->columns.push_back(attr);
            if (attr == 1) {
                containsOne = true;
            }
        }
        if (containsOne == false) {
            options->columns.push_back(1);
        }

        sort(options->columns.begin(), options->columns.end());

        options->batch_capacity = queryOptions->batchCapacity;
    }

    *readOptions = options;
}

bool RangeQuery(void* db, void* range, void** readOptions, size_t* bufLen,
                void** result) {
    ReadOptions* ro = static_cast<ReadOptions*>(*readOptions);
    Range* r = static_cast<Range*>(range);
    list<RangeQueryKeyVal>* res = new list<RangeQueryKeyVal>;
    Status s;
    ro->splitter = new PipeSplitter();
    bool ret = static_cast<DB*>(db)->RangeQuery(*ro, *r, *res, &s);
    delete ro->splitter;
    pfree(range);

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
}
#endif

}
