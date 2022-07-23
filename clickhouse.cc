#include "clickhouse.hh"
#include "clickhouse/client.h"
#include "clickhouse/exceptions.h"

#include <cstdlib>
#include <sstream>
#include <stdexcept>
#include <cassert>
#include <iostream>
#include <string>

#include <regex>

using namespace std;

info_parser::info_parser(const std::string& info) {
    options["username"] = "default";
    options["password"] = "";

    std::regex optregex("(host|port|username|password)(?:=((?:.|\n)*))?");
    
    std::stringstream ss(info);
    std::string token;

    while (ss >> token) {
        std::smatch match;
        std::string s(token);
        if (regex_match(s, match, optregex)) {
            options[string(match[1])] = match[2];
        } else {
            std::cerr << "Cannot parse option: " << token << std::endl;
            exit(EXIT_FAILURE);
        }
    }
}

clickhouse_connection::clickhouse_connection(const std::string& info)
    : parser(info)
    , client{clickhouse::ClientOptions()
        .SetHost(parser.options["host"])
        .SetPort(stoi(parser.options["port"]))
        .SetUser(parser.options["username"])
        .SetPassword(parser.options["password"])} {}

void clickhouse_connection::query(const std::string& query) {
    try {
        client.Execute(query);
    } catch(const clickhouse::ServerError& err) {
        // std::cerr << err.what() << "\n";
    } catch(...) {}
}

void clickhouse_connection::checkAliveness() {
    client.Ping();
}

void schema_clickhouse::initTypes() {
    cerr << "init booltype, inttype, internaltype, arraytype here" << endl;
	booltype = sqltype::get("UInt8");
	inttype = sqltype::get("UInt64");
	internaltype = sqltype::get("internal");
	arraytype = sqltype::get("Array");
}

void schema_clickhouse::initTables() {
    cerr << "Loading tables from database:" << endl;
	// string qry = "select t.name, s.name, t.system, t.type from sys.tables t,  sys.schemas s where t.schema_id=s.id ";
	// MapiHdl hdl = mapi_query(dbh,qry.c_str());
	// while (mapi_fetch_row(hdl)) {
	// 	tables.push_back(table(mapi_fetch_field(hdl,0),mapi_fetch_field(hdl,1),strcmp(mapi_fetch_field(hdl,2),"false")==0 ? true : false , atoi(mapi_fetch_field(hdl,3))==0 ? false : true));
	// }
	// mapi_close_handle(hdl);
    tables.push_back(table("test", "db", true, false));
	cerr << " done." << endl;

	cerr << "Loading columns and constraints...";
	// for (auto t = tables.begin(); t!=tables.end(); t++) {
	// 	string q("select col.name,"
	// 		" col.type "
	// 		" from sys.columns col, sys.tables tab"
	// 		" where tab.name= '");
	// 	q += t->name;
	// 	q += "' and tab.id = col.table_id";

	// 	hdl = mapi_query(dbh,q.c_str());
	// 	while (mapi_fetch_row(hdl)) {
	// 		column c(mapi_fetch_field(hdl,0), sqltype::get(mapi_fetch_field(hdl,1)));
	// 		t->columns().push_back(c);
	// 	}
	// 	mapi_close_handle(hdl);
	// }
	// // TODO: confirm with Martin or Stefan about column
	// // constraints in MonetDB
    tables.front().columns().push_back(column("first", sqltype::get("UInt64")));
    tables.front().columns().push_back(column("second", sqltype::get("UInt64")));
	cerr << " done." << endl;
}

void schema_clickhouse::initOperators() {
    cerr << "Loading operators...";
	// string opq("select f.func, a.type, b.type, c.type"
	// 	" from sys.functions f, sys.args a, sys.args b, sys.args c"
    //             "  where f.id=a.func_id and f.id=b.func_id and f.id=c.func_id and a.name='arg_1' and b.name='arg_2' and c.number=0");
	// hdl = mapi_query(dbh,opq.c_str());
	// while (mapi_fetch_row(hdl)) {
	// 	op o(mapi_fetch_field(hdl,0),sqltype::get(mapi_fetch_field(hdl,1)),sqltype::get(mapi_fetch_field(hdl,2)),sqltype::get(mapi_fetch_field(hdl,3)));
	// 	register_operator(o);
	// }
	// mapi_close_handle(hdl);
    op o("+", sqltype::get("UInt64"), sqltype::get("UInt64"), sqltype::get("UInt64"));
    register_operator(o);
    
    o = op("-", sqltype::get("UInt64"), sqltype::get("UInt64"), sqltype::get("UInt64"));
    register_operator(o);

    o = op("*", sqltype::get("UInt64"), sqltype::get("UInt64"), sqltype::get("UInt64"));
    register_operator(o);
    
    o = op("/", sqltype::get("UInt64"), sqltype::get("UInt64"), sqltype::get("UInt64"));
    register_operator(o);

	cerr << " done." << endl;
}

void schema_clickhouse::initRoutines() {
    // cerr << "Loading routines...";
	// // string routq("select s.name, f.id, a.type, f.name from sys.schemas s, sys.args a, sys.types t, sys.functions f where f.schema_id = s.id and f.id=a.func_id and a.number=0 and a.type = t.sqlname and f.mod<>'aggr'");
	// // hdl = mapi_query(dbh,routq.c_str());
	// // while (mapi_fetch_row(hdl)) {
	// // 	routine proc(mapi_fetch_field(hdl,0),mapi_fetch_field(hdl,1),sqltype::get(mapi_fetch_field(hdl,2)),mapi_fetch_field(hdl,3));
	// // 	register_routine(proc);
	// // }
	// // mapi_close_handle(hdl);
	// routine proc("db", "RandomNumberSpec", sqltype::get("UInt64"), "RandomNumber");
	// register_routine(proc);

    // proc = routine("db", "AbsSpec", sqltype::get("UInt64"), "Abs");
	// register_routine(proc);
    
    // cerr << " done." << endl;
    // cerr << "Loading routine parameters...";
	// // for (auto &proc : routines) {
	// // 	string routpq ("select a.type from sys.args a,"
	// // 		       " sys.functions f "
	// // 		       " where f.id = a.func_id and a.number <> 0 and f.id = '");
	// // 	routpq += proc.specific_name;
	// // 	routpq += "'";
	// // 	hdl = mapi_query(dbh,routpq.c_str());
	// // 	while (mapi_fetch_row(hdl)) {
	// // 		proc.argtypes.push_back(sqltype::get(mapi_fetch_field(hdl,0)));
	// // 	}
	// // 	mapi_close_handle(hdl);
	// // }

    // routines[0].argtypes.push_back(sqltype::get("UInt64"));
    // routines[1].argtypes.push_back(sqltype::get("UInt64"));

	cerr << " done."<< endl;
}

void schema_clickhouse::initAggregates() {
    cerr << "Loading aggregates...";
	// string aggq("select s.name, f.id, a.type, f.name from sys.schemas s, sys.args a, sys.types t, sys.functions f where f.schema_id = s.id and f.id=a.func_id and a.number=0 and a.type = t.sqlname and f.mod='aggr'");

	// hdl = mapi_query(dbh,aggq.c_str());
	// while (mapi_fetch_row(hdl)) {
	// 	routine proc(mapi_fetch_field(hdl,0),mapi_fetch_field(hdl,1),sqltype::get(mapi_fetch_field(hdl,2)),mapi_fetch_field(hdl,3));
	// 	register_aggregate(proc);
	// }
	// mapi_close_handle(hdl);
	cerr << " done." << endl;

	cerr << "Loading aggregates parameters...";
	// for (auto &proc: aggregates) {
	// 	string aggpq ("select a.type from sys.args a, sys.functions f "
	// 		      "where f.id = a.func_id and a.number <> 0 and f.id = '");
	// 	aggpq += proc.specific_name;
	// 	aggpq += "'";
	// 	hdl = mapi_query(dbh,aggpq.c_str());
	// 	while (mapi_fetch_row(hdl)) {
	// 		proc.argtypes.push_back(sqltype::get(mapi_fetch_field(hdl,0)));
	// 	}
	// 	mapi_close_handle(hdl);
	// }
	cerr << " done."<< endl;

	// mapi_destroy(dbh);
	// generate_indexes();
}

//load schema from MonetDB
schema_clickhouse::schema_clickhouse(const std::string& info) : clickhouse_connection(info) {
    initTypes();
    initTables();
    initOperators();
    initRoutines();
    initAggregates();

	cerr << "print loaded information to check correctness" << endl;
	cerr << "Loaded tables.... " << endl;
  	for (auto item : tables) {
		cerr << item.name << "; " << item.schema << "; " << item.is_insertable << "; " << item.is_base_table << endl;
  	}

	cerr << "Loaded columns... " << endl;
  	for (auto tab : tables) {
		for (auto col: tab.columns())
			cerr << tab.name << "; " << col.name << "; "<<col.type->name << endl;
	}

	cerr << "Loaded aggregates and parameters... " << endl;
 	for (auto &proc : aggregates) {
		cerr << proc.specific_name << "; " << proc.schema << "; " << proc.name <<"; " << proc.restype->name ;
		for (auto item : proc.argtypes)
			cerr << "; " << item->name;
		cerr << endl;
 	}
}

dut_clickhouse::dut_clickhouse(const std::string& info) : clickhouse_connection(info) {}

void dut_clickhouse::test(const std::string& query) {
    dut_clickhouse::query(query);
    checkAliveness();
}
