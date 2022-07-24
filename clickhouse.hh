#pragma once

#include <cstdint>
#include <string.h>

#include <clickhouse/client.h>
#include <string>
#include <vector>

#include "dut.hh"
#include "relmodel.hh"
#include "schema.hh"

struct info_parser {
    info_parser(const std::string& info);

    std::map<std::string, std::string> options;
};

class clickhouse_connection {
public:
    clickhouse_connection(const std::string& info);

    void query(const std::string& query);

    void checkAliveness();

    info_parser parser;
    clickhouse::Client client;

};

class schema_clickhouse : public schema, public clickhouse_connection {
public:
    schema_clickhouse(const std::string& info);
    
    std::string quote_name(const std::string &id) override {
        return "\'" + id + "\'";
    }

private:
    void initTypes();
    void initTables();
    void initOperators();
    void initRoutines();
    void initAggregates();

private:
    std::vector<std::string> getTableList();
    void describeTable(const std::string& name);

    void addOperators(
        const std::vector<std::string>& names, 
        const std::string& lType, 
        const std::string& rType,
        const std::string& resultType);

    void addRoutine(
        const std::string& name,
        const std::string& resType,
        const std::vector<std::string>& args);

    void printInfo();

private:
    const std::vector<std::string> numOps = {"+", "-", "*", "/", "=", "!=", "<", "<=", ">=", ">"};
    const std::vector<std::string> stringOps = {"||", "=", "!=", "<", "<=", ">=", ">"};

    const std::vector<std::string> integerTypes = {
                                                    "Int8", "UInt8",
                                                    "Int16", "UInt16",
                                                    "Int32", "UInt32",
                                                    "Int64", "UInt64"
                                                    };


};

class dut_clickhouse : public dut_base, public clickhouse_connection {
public:
    dut_clickhouse(const std::string& info);

    void test(const std::string& query) override;
};
