
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
using namespace rocksdb;

#include "asuraapi.h"
using namespace std;

extern "C" {
#include "postgres.h"

std::string kDBPath = "/tmp/rocksdb_simple_example";

void* dbopen() {
    DB* db;
    Options options;
    options.IncreaseParallelism();
    options.create_if_missing = true;
    Status s = DB::Open(options, kDBPath, &db);
    assert(s.ok());
    return db;
}

void dbclose(void* db) {
    db = static_cast<DB*>(db);
    delete db;
}

uint64_t dbcount(void* db) {
    db = static_cast<DB*>(db);
    string num;
    db->GetProperty("rocksdb.estimate-num-keys", &num);
    return stoull(num);
}

void* getiter(void* db) {
    Iterator* it = db->NewIterator(ReadOptions());
    return it;
}

void delcur(void* it) {
    it = static_cast<Iterator*>(it);
    delete it;
}

bool next(void* db, void* it, char** key, char** value) {
    it = static_cast<Iterator*>(it);
    if (!it->Valid()) return false;
    *key = (char*) palloc(it->key().size()+1);
    *value = (char*) palloc(it->value().size()+1);
    strcpy(*key, it->key().data());
    strcpy(*value, it->value().data());
    return true;
}

bool get(void* db, char* key, char** value) {
    db = static_cast<DB*>(db);
    string skey(key), sval;
    Status s = db->Get(ReadOptions(), key, &sval);
    if (!s.ok()) return false;
    *value = (char*) palloc(sval.length()+1);
    strcpy(*value, sval.c_str());
    return true;
}

bool add(void* db, char* key, char* value) {
    db = static_cast<DB*>(db);
    string skey(key), sval(value);
    Status s = db->Put(WriteOptions(), skey, sval);
    return s.ok()? true: false;
}

bool remove(void* db, char* key) {
    db = static_cast<DB*>(db);
    string skey(key);
    Status s = db->Delete(WriteOptions(), skey);
    return s.ok()? true: false;
}

}
