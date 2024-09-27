//
// Created by noname on 23/09/2024.
//
#ifndef TAOS_C_SRC_MSG_H
#define TAOS_C_SRC_MSG_H

#include "nifpp.h"


enum class TAOS_FUNC {
    CONNECT = 0,
    CLOSE,
    OPTION,
    GET_SERVER_INFO,
    GET_CLIENT_INFO,
    GET_CURRENT_DB,
    SELECT_DB,
    OPTIONS,
    QUERY,
    QUERY_A,
    STMT_PREPARE,
    STMT_EXECUTE,
    STMT_CLOSE,
    FUNC_MAX
};

struct TaskConnectArgs {
    std::string host;
    int port;
    std::string user;
    std::string passwd;
    int is_hash;
    std::string database;
    explicit TaskConnectArgs(std::string& _host, int &_port, std::string& _user, std::string& _passwd, int &_is_hash, std::string& _database);
    ~TaskConnectArgs() = default;

    static TaskConnectArgs* parse(ErlNifEnv* env, ERL_NIF_TERM raw);
};

struct TaskQueryArgs {
    std::string sql;
    explicit TaskQueryArgs(std::string& _sql);
    static TaskQueryArgs* parse(ErlNifEnv* env, ERL_NIF_TERM raw);
};

struct TaskExecuteArgs {
    int num_row;
    std::vector<int> types;
    std::vector<std::tuple<int, std::vector<int>, nifpp::vec_bin>> params;
    explicit TaskExecuteArgs(int& _num_row, std::vector<int>& _types,
                             std::vector<std::tuple<int, std::vector<int32_t>, ErlNifBinary>>& _params);
    static TaskExecuteArgs* parse(ErlNifEnv* env, ERL_NIF_TERM raw);
};

struct ErlTask {
public:
    nifpp::str_atom cb_name;
    uint64_t cb_id;
    int conn;
    int stmt;
    TAOS_FUNC func;
    void* args;

    explicit ErlTask(nifpp::str_atom& cb_pid, uint64_t& cb_id, int& conn, int& stmt, TAOS_FUNC func, void* args);
    ~ErlTask();

    static ErlTask* create(ErlNifEnv* env, const ERL_NIF_TERM* argv);
};

#endif //TAOS_C_SRC_MSG_H
