
#include "rocksdb/db.h"
#include "rocksdb/options.h"
using namespace rocksdb;
using namespace std;

#include "kv.h"

extern "C" {

#include "postgres.h"

void* Open(char* path) {
    DB* db = nullptr;
    Options options;
    options.IncreaseParallelism();
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

uint64_t Count(void* db) {
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

bool Next(void* db, void* iter, char** key, char** value) {
    Iterator* it = static_cast<Iterator*>(iter);
    if (!it->Valid()) return false;
    size_t keyLen = it->key().size() + 1, valLen = it->value().size() + 1;
    *key = (char*) palloc0(keyLen);
    *value = (char*) palloc0(valLen);
    // assuming key & val in storage engine end without trailing zero
    strncpy(*key, it->key().data(), keyLen-1);
    strncpy(*value, it->value().data(), valLen-1);
    it->Next();
    return true;
}

bool Get(void* db, char* key, char** value) {
    string sval;
    Status s = static_cast<DB*>(db)->Get(ReadOptions(), key, &sval);
    if (!s.ok()) return false;
    *value = (char*) palloc0(sval.length()+1);
    strncpy(*value, sval.c_str(), sval.length());
    return true;
}

bool Put(void* db, char* key, char* value) {
    Status s = static_cast<DB*>(db)->Put(WriteOptions(), key, value);
    return s.ok()? true: false;
}

bool Delete(void* db, char* key) {
    Status s = static_cast<DB*>(db)->Delete(WriteOptions(), key);
    return s.ok()? true: false;
}

}
