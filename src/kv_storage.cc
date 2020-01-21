
#ifdef VidarDB
#include "vidardb/db.h"
#include "vidardb/options.h"
using namespace vidardb;
#else
#include "rocksdb/db.h"
#include "rocksdb/options.h"
using namespace rocksdb;
#endif
using namespace std;

#include "kv_storage.h"

extern "C" {

void* Open(char* path) {
    DB* db = nullptr;
    Options options;
    options.create_if_missing = true;
    Status s = DB::Open(options, string(path), &db);
    assert(s.ok());
    return db;
}

void Close(void* db) {
    if (db) {
        delete static_cast<DB*>(db);
    }
}

uint64 Count(void* db) {
    string num;
    static_cast<DB*>(db)->GetProperty("rocksdb.estimate-num-keys", &num);
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

bool Next(void* db, void* iter, char** key, uint32* keyLen,
          char** val, uint32* valLen) {
    Iterator* it = static_cast<Iterator*>(iter);
    if(it == NULL) {
        return false;
    }
    if (!it->Valid()) return false;
 
    *keyLen = it->key().size(), *valLen = it->value().size();
    *key = (char*) palloc0(*keyLen);
    *val = (char*) palloc0(*valLen);
 
    memcpy(*key, it->key().data(), *keyLen);
    memcpy(*val, it->value().data(), *valLen);
  
    it->Next();
    return true;
}

bool Get(void* db, char* key, uint32 keyLen, char** val, uint32* valLen) {
    string sval;
    Status s = static_cast<DB*>(db)->Get(ReadOptions(), Slice(key, keyLen), &sval);
    if (!s.ok()) return false;
    *valLen = sval.length();
    *val = (char*) palloc0(*valLen);
    memcpy(*val, sval.c_str(), *valLen);
    return true;
}

bool Put(void* db, char* key, uint32 keyLen, char* val, uint32 valLen) {
    Status s = static_cast<DB*>(db)->Put(WriteOptions(), Slice(key, keyLen),
                                         Slice(val, valLen));
    return s.ok();
}

bool Delete(void* db, char* key, uint32 keyLen) {
    Status s = static_cast<DB*>(db)->Delete(WriteOptions(), Slice(key, keyLen));
    return s.ok();
}

#ifdef VidarDB
//char** valArray, uint32** valLens, uint32* valArraySize
bool RangeQuery(void* db, void** readOptions, RangeSpec range, pid_t pid, size_t* buffSize) {
    Range r(Slice(range.start, range.startLen), Slice(range.limit, range.limitLen));
    ReadOptions *options = static_cast<ReadOptions*>(*readOptions);
    if (options == nullptr)
    {
        options = (ReadOptions*) palloc0(sizeof(ReadOptions));
    }
    options->batch_capacity = BATCHCAPACITY;
    *readOptions = options;
    
    std::vector<RangeQueryKeyVal> res;
    Status s; 
    bool ret = false; //static_cast<DB*>(db)->RangeQuery(*options, r, res, &s);

    if (!s.ok()) {
        *buffSize = 0;
        return false;
    }
    
    size_t valArraySize = res.size();
    if(valArraySize > 0) {
        char filename[FILENAMELENGTH];
        snprintf(filename, FILENAMELENGTH, "%s%d", RANGEQUERYFILE, pid);
        int fd = ShmOpen(filename, O_RDWR, PERMISSION, __func__);
        *buffSize = (1 + valArraySize) * sizeof(size_t);
        size_t total = 0;
        for (auto it = res.begin(); it != res.end(); ++it) {
            total += (*it).user_key.size();
            total += (*it).user_val.size();
            total += 2*sizeof(size_t);
        }
        *buffSize += total;

        char *rqbuff = (char*) Mmap(NULL,
                        *buffSize,
                        PROT_READ | PROT_WRITE,
                        MAP_SHARED,
                        fd,
                        0,
                        __func__);
        Fclose(fd, __func__);

        char *buffPtr = rqbuff;
        memcpy(buffPtr, &valArraySize, sizeof(size_t));
        buffPtr += sizeof(size_t);
     
        for (auto it = res.begin(); it != res.end(); ++it) {
            size_t keyLen = (*it).user_key.size();
            memcpy(buffPtr, &keyLen, sizeof(size_t));
            buffPtr += sizeof(size_t);
            memcpy(buffPtr, (*it).user_key.c_str(), keyLen);
            buffPtr += keyLen;
            
            size_t valLen = (*it).user_val.size();
            memcpy(buffPtr, &valLen, sizeof(size_t));
            buffPtr += sizeof(size_t);
            memcpy(buffPtr, (*it).user_val.c_str(), valLen);
            buffPtr += valLen;
        }
    }
    return ret;
}
#endif

}
