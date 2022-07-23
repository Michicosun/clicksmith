#pragma once

#include <cstdint>
#include <string.h>

#include <clickhouse/client.h>
#include <string>

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

private:
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

};

class dut_clickhouse : public dut_base, public clickhouse_connection {
public:
	dut_clickhouse(const std::string& info);

    void test(const std::string& query) override;
};
