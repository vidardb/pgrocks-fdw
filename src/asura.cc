
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
using namespace rocksdb;

#include "asuraapi.h"
using namespace std;

extern "C" {
#include "postgres.h"

std::string kDBPath = "/tmp/rocksdb_simple_example";

/**
 * Create a database object.
 */
KVDB *dbnew() {
    DB* db;
    Options options;
    options.IncreaseParallelism();
    options.create_if_missing = true;
    Status s = DB::Open(options, kDBPath, &db);
    assert(s.ok());
    return (KVDB *)db;
}

/**
 * Destroy a database object.
 */
void dbdel(KVDB *db) {
    DB *pdb = (DB *)db;
    delete pdb;
}

/**
 * Open a database file.
 */
bool dbopen(KVDB *db, const char *host, int32_t port, double timeout) {
    DB *pdb = (DB *)db;
    return pdb->open(host, port, timeout);
}

/**
 * Close the database file.
 */
bool dbclose(KVDB *db) {
    DB *pdb = (DB *)db;
    return pdb->close();
}

/*
 * get a count of the number of keys
 */
int64_t dbcount(KVDB *db) {
    DB *pdb = (DB *)db;
    return pdb->count();
}

CUR *getcur(KVDB *db) {
    DB *pdb = (DB *)db;
    RemoteDB::Cursor *cur = pdb->cursor();
    cur->jump();
    return (CUR *) cur;
}

void delcur(CUR *cur) {
    DB::Cursor *rcur = (DB::Cursor *) cur;
    delete rcur;
}

bool next(KVDB *db, CUR *cur, char **key, char **value) {
    std::string skey;
    std::string sval;
    DB::Cursor *rcur = (DB::Cursor *) cur;
    bool res = rcur->get(&skey, &sval, NULL, true);
    if (!res) return false;
    *key = (char *) palloc(sizeof(char)*(skey.length()+1));
    *value = (char *) palloc(sizeof(char)*(sval.length()+1));
    std::strcpy(*key, skey.c_str());
    std::strcpy(*value, sval.c_str());
    return true;
}

bool get(KVDB *db, char *key, char **value) {
    std::string skey(key);
    std::string sval;
    DB *pdb = (DB *)db;
    if (!pdb->get(skey, &sval)) return false;
    *value = (char *) palloc(sizeof(char)*(sval.length()+1));
    std::strcpy(*value, sval.c_str());
    return true;
}

bool add(KVDB *db, const char *key, const char *value) {
    DB *pdb = (DB *)db;
    std::string skey(key);
    std::string sval(value);
    return pdb->add(skey, sval);
}

bool replace(KVDB *db, const char *key, const char *value) {
    DB *pdb = (DB *)db;
    std::string skey(key);
    std::string sval(value);
    return pdb->replace(skey, sval);
}

bool remove(KVDB *db, const char *key) {
    DB *pdb = (DB *)db;
    std::string skey(key);
    return pdb->remove(skey);
}

const char *geterror(KVDB *db) {
    DB *pdb = (DB *)db;
    return pdb->error().name();
}

const char *geterrormsg(KVDB *db) {
    DB *pdb = (DB *)db;
    return pdb->error().message();
}

}
