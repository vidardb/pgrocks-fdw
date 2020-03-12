
#ifdef VidarDB
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

#ifdef VidarDB
void* Open(char* path, bool useColumn, int columnNum) {
    DB* db = nullptr;
    Options options;
    options.OptimizeAdaptiveLevelStyleCompaction();
    options.create_if_missing = true;
    
    int knob = -1;
    
    if (useColumn) {
        knob = 0;
    }

    shared_ptr<TableFactory> block_based_table(NewBlockBasedTableFactory());
    shared_ptr<TableFactory> column_table(NewColumnTableFactory());
    ColumnTableOptions* column_opts =
        static_cast<ColumnTableOptions*>(column_table->GetOptions());
    column_opts->column_num = columnNum;
    
    options.table_factory.reset(NewAdaptiveTableFactory(block_based_table,
        block_based_table, column_table, knob));

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
    if (db) {
        delete static_cast<DB*>(db);
    }
}

uint64 Count(void* db) {
    string num;
    #ifdef VidarDB
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
    if (it) {
        delete static_cast<Iterator*>(it);
    }
}

bool Next(void* db, void* iter, char** key, size_t* keyLen,
          char** val, size_t* valLen) {
    Iterator* it = static_cast<Iterator*>(iter);
    if (it == nullptr) return false;
    if (!it->Valid()) return false;
 
    *keyLen = it->key().size(), *valLen = it->value().size();
    *key = (char*) palloc0(*keyLen);
    *val = (char*) palloc0(*valLen);
 
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
    *val = (char*) palloc0(*valLen);
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

#ifdef VidarDB
bool RangeQuery(void* db, void** readOptions, RangeQueryOptions* rangeQueryOptions, pid_t pid, size_t* buffSize) {
    printf("\n-----------------%s----------------------\n", __func__);
    Range r;
    if (rangeQueryOptions != NULL) {
        if (rangeQueryOptions->startLen > 0) {
            Slice s(rangeQueryOptions->start, rangeQueryOptions->startLen);
            r.start = s;
        }
        if (rangeQueryOptions->limitLen > 0) {
            Slice l(rangeQueryOptions->limit, rangeQueryOptions->limitLen);
            r.limit = l;
        }
    }

    ReadOptions *options = static_cast<ReadOptions*>(*readOptions);
    if (options == NULL) {
        options = (ReadOptions*) palloc0(sizeof(ReadOptions));
    }

    /*first attribute in the value must be returned*/
    options->columns.push_back(1);
    if (rangeQueryOptions != NULL) {
        for(size_t optionIdx = 0; optionIdx < rangeQueryOptions->targetNum; optionIdx++) {
            uint32_t targetIdx = *(rangeQueryOptions->targetIndexes + optionIdx);
            if (targetIdx > 1) {
                options->columns.push_back(targetIdx);
            }
        }
    }    
    options->batch_capacity = BATCHCAPACITY;
    if (rangeQueryOptions->batchCapacity > 0) {
        options->batch_capacity = rangeQueryOptions->batchCapacity;
    }
    *readOptions = options;

    std::list<RangeQueryKeyVal> res;
    Status s;
    bool ret = static_cast<DB*>(db)->RangeQuery(*options, r, res, &s);

    if (!s.ok()) {
        *buffSize = 0;
        return false;
    }

    size_t valArraySize = res.size();

    if(valArraySize > 0) {
        char filename[FILENAMELENGTH];
        snprintf(filename, FILENAMELENGTH, "%s%d", RANGEQUERYFILE, pid);
        ShmUnlink(filename, __func__);
        int fd = ShmOpen(filename,
                         O_CREAT | O_RDWR | O_EXCL,
                         PERMISSION,
                         __func__);
        
    
        size_t total = 0;
        for (auto it = res.begin(); it != res.end(); ++it) {
            total += (*it).user_key.size();
            total += (*it).user_val.size();
            total += 2*sizeof(size_t);
        }
        *buffSize = total;

        char *rangeQueryBuffer = (char*) Mmap(NULL,
                                              *buffSize,
                                              PROT_READ | PROT_WRITE,
                                              MAP_SHARED,
                                              fd,
                                              0,
                                              __func__);
        Ftruncate(fd, *buffSize, __func__);
        Fclose(fd, __func__);
        
        char *buffPtr = rangeQueryBuffer;

        for (auto it = res.begin(); it != res.end(); ++it) {
            size_t keyLen = (*it).user_key.size();
            memcpy(buffPtr, &keyLen, sizeof(keyLen));
            buffPtr += sizeof(keyLen);
            memcpy(buffPtr, (*it).user_key.c_str(), keyLen);
            buffPtr += keyLen;

            size_t valLen = (*it).user_val.size();
            memcpy(buffPtr, &valLen, sizeof(valLen));
            buffPtr += sizeof(valLen);
            memcpy(buffPtr, (*it).user_val.c_str(), valLen);
            buffPtr += valLen;
        }

        Munmap(rangeQueryBuffer, *buffSize, __func__);
    } 

    return ret;
}
#endif

}
