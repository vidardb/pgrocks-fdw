
#ifdef VIDARDB
#include "vidardb/db.h"
#include "vidardb/options.h"
#include "vidardb/table.h"
using namespace vidardb;
#include <list>
#else
#include "rocksdb/db.h"
#include "rocksdb/options.h"
using namespace rocksdb;
#endif
using namespace std;

#include "kv_storage.h"

extern "C" {

#ifdef VIDARDB
void* Open(char* path, bool useColumn, uint32_t columnNum) {
    DB* db = nullptr;
    Options options;
    options.OptimizeAdaptiveLevelStyleCompaction();
    options.create_if_missing = true;

    shared_ptr<TableFactory> block_based_table(NewBlockBasedTableFactory());
    shared_ptr<TableFactory> column_table(NewColumnTableFactory());
    ColumnTableOptions* column_opts =
        static_cast<ColumnTableOptions*>(column_table->GetOptions());
    column_opts->column_num = columnNum;  // Total column number excluding key

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
    string num;
    #ifdef VIDARDB
    static_cast<DB*>(db)->GetProperty("vidardb.estimate-num-keys", &num);
    #else
    static_cast<DB*>(db)->GetProperty("rocksdb.estimate-num-keys", &num);
    #endif
    return stoull(num);
}

void* GetIter(void* db) {
    Iterator* it = static_cast<DB*>(db)->NewIterator(ReadOptions());
    it->SeekToFirst();
    return it;
}

void DelIter(void* it) {
    delete static_cast<Iterator*>(it);
}

bool Next(void* db, void* iter, char** key, size_t* keyLen,
          char** val, size_t* valLen) {
    Iterator* it = static_cast<Iterator*>(iter);
    if (it == nullptr || !it->Valid()) return false;

    *keyLen = it->key().size(), *valLen = it->value().size();
    *key = static_cast<char*>(palloc0(*keyLen));
    *val = static_cast<char*>(palloc0(*valLen));

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
    *val = static_cast<char*>(palloc0(*valLen));
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
bool RangeQuery(void* db, void** readOptions, RangeQueryOptions* queryOptions,
                pid_t pid, size_t* bufLen) {
    printf("\n-----------------%s----------------------\n", __func__);
    Range r;
    if (queryOptions != NULL) {
        if (queryOptions->startLen > 0) {
            Slice s(queryOptions->start, queryOptions->startLen);
            r.start = s;
        }
        if (queryOptions->limitLen > 0) {
            Slice l(queryOptions->limit, queryOptions->limitLen);
            r.limit = l;
        }
    }

    ReadOptions* options = static_cast<ReadOptions*>(*readOptions);
    if (options == NULL) {
        options = static_cast<ReadOptions*>(palloc0(sizeof(ReadOptions)));
    }

    /* first attribute in the value must be returned */
    options->columns.push_back(1);
    if (queryOptions != NULL) {
        for (size_t i = 0; i < queryOptions->targetNum; i++) {
            uint32_t targetIdx = *(queryOptions->targetIndexes + i);
            if (targetIdx > 1) {
                options->columns.push_back(targetIdx);
            }
        }
    }
    options->batch_capacity = queryOptions->batchCapacity > 0?
                              queryOptions->batchCapacity: BATCHCAPACITY;
    *readOptions = options;

    std::list<RangeQueryKeyVal> res;
    Status s;
    bool ret = static_cast<DB*>(db)->RangeQuery(*options, r, res, &s);

    if (!s.ok()) {
        *bufLen = 0;
        return false;
    }

    if (res.size() == 0) return ret;

    char filename[FILENAMELENGTH];
    snprintf(filename, FILENAMELENGTH, "%s%d", RANGEQUERYFILE, pid);
    ShmUnlink(filename, __func__);
    int fd = ShmOpen(filename,
                     O_CREAT | O_RDWR | O_EXCL,
                     PERMISSION,
                     __func__);

    /* TODO: will be provided by storage engine later */
    size_t total = 0;
    for (std::list<RangeQueryKeyVal>::iterator it = res.begin();
         it != res.end(); ++it) {
        total += it->user_key.size() + it->user_val.size() + sizeof(size_t) * 2;
    }
    *bufLen = total;

    char *buf = static_cast<char*>(Mmap(NULL,
                                   *bufLen,
                                   PROT_READ | PROT_WRITE,
                                   MAP_SHARED,
                                   fd,
                                   0,
                                   __func__));
    Ftruncate(fd, *bufLen, __func__);
    Fclose(fd, __func__);

    for (auto it = res.begin(); it != res.end(); ++it) {
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

    Munmap(buf, *bufLen, __func__);
    // ShmUnlink(filename, __func__); Do we need this here?

    return ret;
}
#endif

}
