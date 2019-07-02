
#include <ktremotedb.h>

#include "asuraapi.h"

using namespace kyototycoon;

extern "C" {
#include "postgres.h"

/**
 * Create a database object.
 */
DB *dbnew() {
    return (DB *)new RemoteDB;
}


/**
 * Destroy a database object.
 */
void dbdel(DB *db) {
    _assert_(db);
    RemoteDB *pdb = (RemoteDB *)db;
    delete pdb;
}

/**
 * Open a database file.
 */
bool dbopen(DB *db, const char *host, int32_t port, double timeout) {
    _assert_(db && host && port && timeout);
    RemoteDB *pdb = (RemoteDB *)db;
    return pdb->open(host, port, timeout);
}

/**
 * Close the database file.
 */
bool dbclose(DB *db) {
    _assert_(db);
    RemoteDB *pdb = (RemoteDB *)db;
    return pdb->close();
}

/*
 * get a count of the number of keys
 */
int64_t dbcount(DB *db) {
    _assert_(db);
    RemoteDB *pdb = (RemoteDB *)db;
    return pdb->count();
}

CUR *getcur(DB *db) {
    _assert_(db);
    RemoteDB *pdb = (RemoteDB *)db;
    RemoteDB::Cursor *cur = pdb->cursor();
    cur->jump();
    return (CUR *) cur;
}

void delcur(CUR *cur) {
    _assert_(cur);
    RemoteDB::Cursor *rcur = (RemoteDB::Cursor *) cur;
    delete rcur;
}

bool next(DB *db, CUR *cur, char **key, char **value) {
    std::string skey;
    std::string sval;

    RemoteDB::Cursor *rcur = (RemoteDB::Cursor *) cur;
    bool res = rcur->get(&skey, &sval, NULL, true);
    if (!res) return false;

    *key = (char *) palloc(sizeof(char)*(skey.length()+1));
    *value = (char *) palloc(sizeof(char)*(sval.length()+1));

    std::strcpy(*key, skey.c_str());
    std::strcpy(*value, sval.c_str());
    return true;
}

bool get(DB *db, char *key, char **value) {
    _assert_(db && key);

    std::string skey(key);
    std::string sval;
    RemoteDB *pdb = (RemoteDB *)db;

    if (!pdb->get(skey, &sval)) return false;

    *value = (char *) palloc(sizeof(char)*(sval.length()+1));
    std::strcpy(*value, sval.c_str());

    return true;
}

bool add(DB *db, const char *key, const char *value) {
    _assert_(db);
    RemoteDB *pdb = (RemoteDB *)db;

    std::string skey(key);
    std::string sval(value);

    return pdb->add(skey, sval);
}

bool replace(DB *db, const char *key, const char *value) {
    _assert_(db);
    RemoteDB *pdb = (RemoteDB *)db;

    std::string skey(key);
    std::string sval(value);

    return pdb->replace(skey, sval);
}


bool remove(DB *db, const char *key) {
    _assert_(db);
    RemoteDB *pdb = (RemoteDB *)db;

    std::string skey(key);

    return pdb->remove(skey);
}

const char *geterror(DB *db) {
    _assert_(db);
    RemoteDB *pdb = (RemoteDB *)db;
    return pdb->error().name();
}

const char *geterrormsg(DB *db) {
    _assert_(db);
    RemoteDB *pdb = (RemoteDB *)db;
    return pdb->error().message();
}

}
