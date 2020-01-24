
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

bool Next(void* db, void* iter, char** key, uint32* keyLen,
          char** val, uint32* valLen) {
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

}
