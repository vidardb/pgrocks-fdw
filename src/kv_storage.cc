
#include "rocksdb/db.h"
#include "rocksdb/options.h"
using namespace rocksdb;
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
bool RangeQuery(void* db, void** readOptions, RangeSpec range, char** valArray, uint32** valLens, uint32* valArraySize) {
    Range r(Slice(range.start, range.startLen), Slice(range.limit, range.limitLen));
    ReadOptions *options = static_cast<ReadOptions*>(*readOptions);
    if (options == nullptr)
    {
        options = (ReadOptions*) palloc0(sizeof(ReadOptions));
    }
    options->max_result_num = MAXRESULTNUM;
    *readOptions = options;
    std::vector<std::string> res;
    Status s; 
    bool ret = static_cast<DB*>(db)->RangeQuery(*options, r, res, &s);

    if (!s.ok()) {
        *valArraySize = 0;
        return false;
    }
    
    *valArraySize = res.size();
    if( *valArraySize > 0) {
        uint32 *tmpLens = (uint32*) palloc0(*valArraySize * sizeof(uint32));
        *valLens = tmpLens;
        uint32 total = 0;
        for (auto it = res.begin(); it != res.end(); ++it) {
            *tmpLens = (*it).size();
            total += (*it).size();
            tmpLens++;
        }
        char *tmpVal = (char*) palloc0(total);
        *valArray = tmpVal;
        for (auto it = res.begin(); it != res.end(); ++it) {
            memcpy(tmpVal, (*it).c_str(), (*it).size());
            tmpVal = tmpVal + (*it).size();
        }
    }
    return ret;
}
#endif

}
