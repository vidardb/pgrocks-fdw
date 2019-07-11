#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
using namespace rocksdb;

#include "kvapi.h"
#include <iostream>
using namespace std;

extern "C" {
#include "postgres.h"

string kDBPath = "/home/jsc/Desktop/tmp/";

void* Open() {
    cout<<"Open"<<endl;
    DB* db;
    Options options;
    options.IncreaseParallelism();
    options.create_if_missing = true;
    Status s = DB::Open(options, kDBPath, &db);
    assert(s.ok());
    if (!s.ok()) cout<<"Open Error!"<<endl;
    return db;
}

void Close(void* db) {
    if (db) delete static_cast<DB*>(db);
}

uint64_t Count(void* db) {
    string num;
    static_cast<DB*>(db)->GetProperty("rocksdb.estimate-num-keys", &num);
    return stoull(num);
}

void* GetIter(void* db) {
    return static_cast<DB*>(db)->NewIterator(ReadOptions());
}

void DelIter(void* it) {
    if (it) delete static_cast<Iterator*>(it);
}

bool Next(void* db, void* iter, char** key, char** value) {
    Iterator* it = static_cast<Iterator*>(it);
    if (!it->Valid()) return false;
    *key = (char*) palloc(it->key().size()+1);
    *value = (char*) palloc(it->value().size()+1);
    strcpy(*key, it->key().data());
    strcpy(*value, it->value().data());
    return true;
}

bool Get(void* db, char* key, char** value) {
    string skey(key), sval;
    Status s = static_cast<DB*>(db)->Get(ReadOptions(), key, &sval);
    if (!s.ok()) return false;
    *value = (char*) palloc(sval.length()+1);
    strcpy(*value, sval.c_str());
    return true;
}

bool Put(void* db, char* key, char* value) {
    string skey(key), sval(value);
    Status s = static_cast<DB*>(db)->Put(WriteOptions(), skey, sval);
    return s.ok()? true: false;
}

bool Delete(void* db, char* key) {
    string skey(key);
    Status s = static_cast<DB*>(db)->Delete(WriteOptions(), skey);
    return s.ok()? true: false;
}

}
