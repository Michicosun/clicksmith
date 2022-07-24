#include "clickhouse.hh"
#include "clickhouse/client.h"
#include "clickhouse/columns/string.h"
#include "clickhouse/exceptions.h"
#include "dut.hh"
#include "relmodel.hh"

#include <cstddef>
#include <cstdlib>
#include <sstream>
#include <stdexcept>
#include <cassert>
#include <iostream>
#include <string>
#include <string_view>

#include <regex>
#include <vector>

using namespace std;

info_parser::info_parser(const std::string& info) {
    options["user"] = "default";
    options["pass"] = "";
    options["db"] = "db";

    std::regex optregex("(host|port|user|pass|db)(?:=((?:.|\n)*))?");
    
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
        .SetUser(parser.options["user"])
        .SetPassword(parser.options["pass"])} {}

void clickhouse_connection::query(const std::string& query) {
    try {
        client.Execute(query);
    } catch(const clickhouse::ServerError& err) {
        std::cout << err.what() << std::endl;
    } catch(...) {}
}

void clickhouse_connection::checkAliveness() {
    try {
        client.Ping();
    } catch(...) {
        throw dut::failure("ping failed");
    }
}

void schema_clickhouse::initTypes() {
    std::cerr << "init booltype, inttype, internaltype, arraytype here" << std::endl;
    
    booltype = sqltype::get("UInt8");
    inttype = sqltype::get("UInt64");
    internaltype = sqltype::get("internal");
    arraytype = sqltype::get("Array");

    std::cerr << " done." << std::endl;
} 

std::vector<std::string> schema_clickhouse::getTableList() {
    std::string query = "show tables in " + parser.options["db"];

    std::vector<std::string> ans; 
    client.Select(query, [&ans](const clickhouse::Block& block) {
        for (size_t i = 0; i < block.GetRowCount(); ++i) {
            std::string name = std::string(block[0]->As<clickhouse::ColumnString>()->At(i));
            ans.push_back(name);
        }
    });

    return ans;
}

bool startsWith(const std::string& a, const std::string& b) {
    size_t i = 0;
    for (; i < a.size() && i < b.size(); ++i) {
        if (a[i] != b[i]) return false;
    }
    return i == b.size();
}

void schema_clickhouse::describeTable(const std::string& name) {
    std::string query = "describe " + parser.options["db"] + "." + name;

    tables.push_back(table(name, parser.options["db"], true, false));

    client.Select(query, [&table = tables.back()](const clickhouse::Block& block) {
        for (size_t i = 0; i < block.GetRowCount(); ++i) {
            std::string name = std::string(block[0]->As<clickhouse::ColumnString>()->At(i));
            std::string type = std::string(block[1]->As<clickhouse::ColumnString>()->At(i));

            if (startsWith(type, "Array")) {
                table.columns().push_back(column(name, sqltype::get("Array")));
            }
        }
    });
}

void schema_clickhouse::initTables() {
    std::cerr << "Loading tables from database:" << std::endl;
    
    auto tableNames = getTableList();

    for (const auto& name : tableNames) {
        describeTable(name);
    }

    std::cerr << " done." << std::endl;
}

void schema_clickhouse::addOperators(
        const std::vector<std::string>& names, 
        const std::string& lType, 
        const std::string& rType,
        const std::string& resultType)
{
    for (const auto& name : names) {
        op o(name, sqltype::get(lType), sqltype::get(rType), sqltype::get(resultType));
        register_operator(o);
    }
}

void schema_clickhouse::initOperators() {
    std::cerr << "Loading operators...";

    for (const auto& type : integerTypes) {
        addOperators(numOps, type, type, type);
        addOperators({"like"}, type, type, "UInt8");
        addOperators({"not like"}, type, type, "UInt8");
        addOperators({"ilike"}, type, type, "UInt8");
    }

    addOperators(stringOps, "String", "String", "UInt8");
    
    types.push_back(sqltype::get("String"));
    types.push_back(sqltype::get("Array"));
    for (const auto& type : integerTypes) {
        types.push_back(sqltype::get(type));
    }

    std::cerr << " done." << std::endl;
}

void schema_clickhouse::addRoutine(
    const std::string& name,
    const std::string& resType,
    const std::vector<std::string>& args)
{
    routine proc("", name, sqltype::get(resType), name);
    register_routine(proc);

    for (const auto& arg : args) {
        routines.back().argtypes.push_back(sqltype::get(arg));
    }
}

void schema_clickhouse::initRoutines() {
    std::cerr << "Loading routines...";

    for (const auto& type : integerTypes) {
        addRoutine("plus", type, {type, type});
        addRoutine("minus", type, {type, type});
        addRoutine("multiply", type, {type, type});
        addRoutine("divide", type, {type, type});
        addRoutine("intDiv", type, {type, type});
        addRoutine("intDivOrZero", type, {type, type});
        addRoutine("modulo", type, {type, type});
        addRoutine("moduloOrZero", type, {type, type});
        addRoutine("negate", type, {type});
        addRoutine("abs", type, {type});
        addRoutine("gcd", type, {type, type});
        addRoutine("lcm", type, {type, type});
    }

    addRoutine("empty", "UInt8", {"String"});
    addRoutine("length", "UInt64", {"String"});
    addRoutine("lower", "String", {"String"});
    addRoutine("upper", "String", {"String"});
    addRoutine("reverse", "String", {"String"});
    addRoutine("concat", "String", {"String", "String"});
    addRoutine("position", "UInt64", {"String", "String"});
    addRoutine("positionCaseInsensitive", "UInt64", {"String", "String"});
    addRoutine("match", "UInt8", {"String", "String"});

    std::cerr << " done."<< std::endl;
}

void schema_clickhouse::initAggregates() {}

void schema_clickhouse::printInfo() {
    std::cerr << "print loaded information to check correctness" << std::endl;
    std::cerr << "Loaded tables.... " << std::endl;
      for (auto item : tables) {
        std::cerr << item.name << "; " << item.schema << "; " << item.is_insertable << "; " << item.is_base_table << std::endl;
      }

    std::cerr << "Loaded columns... " << std::endl;
      for (auto tab : tables) {
        for (auto col: tab.columns())
            std::cerr << tab.name << "; " << col.name << "; "<<col.type->name << std::endl;
    }

    std::cerr << "Loaded aggregates and parameters... " << std::endl;
     for (auto &proc : aggregates) {
        std::cerr << proc.specific_name << "; " << proc.schema << "; " << proc.name <<"; " << proc.restype->name ;
        for (auto item : proc.argtypes)
            std::cerr << "; " << item->name;
        std::cerr << std::endl;
     }
}

//load schema from MonetDB
schema_clickhouse::schema_clickhouse(const std::string& info) : clickhouse_connection(info) {
    initTypes();
    initTables();
    initOperators();
    initRoutines();
    initAggregates();

    generate_indexes();

    printInfo();
}

dut_clickhouse::dut_clickhouse(const std::string& info) : clickhouse_connection(info) {}

void dut_clickhouse::test(const std::string& query) {
    dut_clickhouse::query(query);
    checkAliveness();
}
