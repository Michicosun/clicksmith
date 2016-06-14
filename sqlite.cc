#include <stdexcept>
#include <cassert>
#include "sqlite.hh"
#include <iostream>

#ifndef HAVE_BOOST_REGEX
#include <regex>
#else
#include <boost/regex.hpp>
using boost::regex;
using boost::smatch;
using boost::regex_match;
#endif

using namespace std;

static regex e_syntax("near \".*\": syntax error");
static regex e_user_abort("callback requested query abort");
  
extern "C"  {
#include <sqlite3.h>
}

extern "C" int my_sqlite3_busy_handler(void *, int)
{
  throw std::runtime_error("sqlite3 timeout");
}

extern "C" int callback(void *arg, int argc, char **argv, char **azColName)
{
  (void)arg;

  int i;
  for(i=0; i<argc; i++){
    printf("%s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL");
  }
  printf("\n");
  return 0;
}

extern "C" int table_callback(void *arg, int argc, char **argv, char **azColName)
{
  (void) argc; (void) azColName;
  auto tables = (vector<table> *)arg;
  tables->push_back(table(argv[2], "main", true, true));
  return 0;
}

extern "C" int column_callback(void *arg, int argc, char **argv, char **azColName)
{
  (void) argc; (void) azColName;
  table *tab = (table *)arg;
  column c(argv[1], argv[2]);
  tab->columns().push_back(c);
  return 0;
}

schema_sqlite::schema_sqlite(std::string &conninfo)
{
  rc = sqlite3_open_v2(conninfo.c_str(), &db, SQLITE_OPEN_READONLY, 0);
  if (rc) {
    throw std::runtime_error(sqlite3_errmsg(db));
  }

  sqlite3_busy_handler(db, my_sqlite3_busy_handler, 0);
//   q("PRAGMA busy_timeout = 1000;");
  
  cerr << "Loading tables...";

  rc = sqlite3_exec(db, "SELECT * FROM main.sqlite_master where type='table'", table_callback, (void *)&tables, &zErrMsg);
  if (rc!=SQLITE_OK) {
    auto e = std::runtime_error(zErrMsg);
    sqlite3_free(zErrMsg);
    throw e;
  }

  cerr << "done." << endl;

  cerr << "Loading columns and constraints...";

  for (auto t = tables.begin(); t != tables.end(); ++t) {
    string q("pragma table_info(");
    q += t->name;
    q += ");";

    rc = sqlite3_exec(db, q.c_str(), column_callback, (void *)&*t, &zErrMsg);
    if (rc!=SQLITE_OK) {
      auto e = std::runtime_error(zErrMsg);
      sqlite3_free(zErrMsg);
      throw e;
    }
  }

  cerr << "done." << endl;

#define BINOP(n,t) do {op o(#n,#t,#t,#t); register_operator(o); } while(0)

  BINOP(||, TEXT);
  BINOP(*, INTEGER);
  BINOP(/, INTEGER);

  BINOP(+, INTEGER);
  BINOP(-, INTEGER);

  BINOP(>>, INTEGER);
  BINOP(<<, INTEGER);

  BINOP(&, INTEGER);
  BINOP(|, INTEGER);

  BINOP(<, INTEGER);
  BINOP(<=, INTEGER);
  BINOP(>, INTEGER);
  BINOP(>=, INTEGER);

  BINOP(=, INTEGER);
  BINOP(<>, INTEGER);
  BINOP(IS, INTEGER);
  BINOP(IS NOT, INTEGER);

  BINOP(AND, INTEGER);
  BINOP(OR, INTEGER);
  
  cerr << "done." << endl;

#define FUNC(n,r) do {							\
    routine proc("", "", sqltype::get(#r), #n);				\
    register_routine(proc);						\
  } while(0)

#define FUNC1(n,r,a) do {						\
    routine proc("", "", sqltype::get(#r), #n);				\
    proc.argtypes.push_back(sqltype::get(#a));				\
    register_routine(proc);						\
  } while(0)

#define FUNC2(n,r,a,b) do {						\
    routine proc("", "", sqltype::get(#r), #n);				\
    proc.argtypes.push_back(sqltype::get(#a));				\
    proc.argtypes.push_back(sqltype::get(#b));				\
    register_routine(proc);						\
  } while(0)

#define FUNC3(n,r,a,b,c) do {						\
    routine proc("", "", sqltype::get(#r), #n);				\
    proc.argtypes.push_back(sqltype::get(#a));				\
    proc.argtypes.push_back(sqltype::get(#b));				\
    proc.argtypes.push_back(sqltype::get(#c));				\
    register_routine(proc);						\
  } while(0)

  FUNC(last_insert_rowid, INTEGER);
  FUNC(random, INTEGER);
  FUNC(sqlite_source_id, TEXT);
  FUNC(sqlite_version, TEXT);
  FUNC(total_changes, INTEGER);

  FUNC1(abs, INTEGER, REAL);
  FUNC1(hex, TEXT, TEXT);
  FUNC1(length, INTEGER, TEXT);
  FUNC1(lower, TEXT, TEXT);
  FUNC1(ltrim, TEXT, TEXT);
  FUNC1(quote, TEXT, TEXT);
  FUNC1(randomblob, TEXT, INTEGER);
  FUNC1(round, INTEGER, REAL);
  FUNC1(rtrim, TEXT, TEXT);
  FUNC1(soundex, TEXT, TEXT);
  FUNC1(sqlite_compileoption_get, TEXT, INTEGER);
  FUNC1(sqlite_compileoption_used, INTEGER, TEXT);
  FUNC1(trim, TEXT, TEXT);
  FUNC1(typeof, TEXT, INTEGER);
  FUNC1(typeof, TEXT, NUMERIC);
  FUNC1(typeof, TEXT, REAL);
  FUNC1(typeof, TEXT, TEXT);
  FUNC1(unicode, INTEGER, TEXT);
  FUNC1(upper, TEXT, TEXT);
  FUNC1(zeroblob, TEXT, INTEGER);

  FUNC2(glob, INTEGER, TEXT, TEXT);
  FUNC2(instr, INTEGER, TEXT, TEXT);
  FUNC2(like, INTEGER, TEXT, TEXT);
  FUNC2(ltrim, TEXT, TEXT, TEXT);
  FUNC2(rtrim, TEXT, TEXT, TEXT);
  FUNC2(trim, TEXT, TEXT, TEXT);
  FUNC2(round, INTEGER, REAL, INTEGER);
  FUNC2(substr, TEXT, TEXT, INTEGER);

  FUNC3(substr, TEXT, TEXT, INTEGER, INTEGER);
  FUNC3(replace, TEXT, TEXT, TEXT, TEXT);


  booltype = sqltype::get("INTEGER");
  inttype = sqltype::get("INTEGER");

  internaltype = sqltype::get("internal");
  arraytype = sqltype::get("ARRAY");

  true_literal = "1";
  false_literal = "0";

  cerr << "Generating indexes...";
  generate_indexes();
  cerr << "done." << endl;
  sqlite3_close(db);
  db = 0;
}

void schema_sqlite::q(const char *query)
{
  rc = sqlite3_exec(db, query, callback, 0, &zErrMsg);
  if( rc!=SQLITE_OK ){
    auto e = std::runtime_error(zErrMsg);
    sqlite3_free(zErrMsg);
    throw e;
  }
}

schema_sqlite::~schema_sqlite()
{
  if (db)
    sqlite3_close(db);
}

dut_sqlite::dut_sqlite(std::string &conninfo)
{
  rc = sqlite3_open_v2(conninfo.c_str(), &db, SQLITE_OPEN_READONLY, 0);
  if (rc) {
    throw std::runtime_error(sqlite3_errmsg(db));
  }
}

extern "C" int dut_callback(void *arg, int argc, char **argv, char **azColName)
{
  (void) arg; (void) argc; (void) argv; (void) azColName;
  return SQLITE_ABORT;
}

void dut_sqlite::test(const std::string &stmt)
{
  rc = sqlite3_exec(db, stmt.c_str(), dut_callback, 0, &zErrMsg);
  if( rc!=SQLITE_OK ){
    try {
      if (regex_match(zErrMsg, e_syntax))
	throw dut::syntax(zErrMsg);
      else if (regex_match(zErrMsg, e_user_abort))
	return;
      else 
	throw dut::failure(zErrMsg);
    } catch (dut::failure &e) {
      sqlite3_free(zErrMsg);
      throw;
    }
  }
}

