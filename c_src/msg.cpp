//
// Created by noname on 23/09/2024.
//
#include "msg.h"

ErlTask::ErlTask(nifpp::str_atom& _cb_pid, uint64_t& cb_id, int& conn, int& stmt, TAOS_FUNC func, void* args): cb_name(_cb_pid) {
    this->cb_id = cb_id;
    this->conn = conn;
    this->stmt = stmt;
    this->func = func;
    this->args = args;
}

ErlTask *ErlTask::create(ErlNifEnv* env, const ERL_NIF_TERM *argv) {
    try {
        nifpp::str_atom cb_pid;
        uint64_t cb_id;
        int conn, stmt, func, func_min, func_max;
        nifpp::get_throws(env, argv[0], cb_pid);
        nifpp::get_throws(env, argv[1], cb_id);
        nifpp::get_throws(env, argv[2], conn);
        nifpp::get_throws(env, argv[3], stmt);
        nifpp::get_throws(env, argv[4], func);
        func_min = static_cast<int>(TAOS_FUNC::CONNECT);
        func_max = static_cast<int>(TAOS_FUNC::FUNC_MAX);
        if (func < func_min || func >= func_max) {
            return nullptr;
        }
        auto func_id = static_cast<TAOS_FUNC>(func);
        void* args = nullptr;
        switch (func_id) {
            case TAOS_FUNC::CONNECT:
                args = TaskConnectArgs::parse(env, argv[5]);
                break;
            case TAOS_FUNC::SELECT_DB:
            case TAOS_FUNC::QUERY:
            case TAOS_FUNC::QUERY_A:
            case TAOS_FUNC::STMT_PREPARE:
                args = TaskQueryArgs::parse(env, argv[5]);
                break;
            case TAOS_FUNC::STMT_EXECUTE:
                args = TaskExecuteArgs::parse(env, argv[5]);
            default:
                break;
        }
        return new ErlTask(cb_pid, cb_id, conn, stmt, func_id, args);
    } catch (...) {
        return nullptr;
    }
}

ErlTask::~ErlTask() {
    if (this->args != nullptr) {
        TaskConnectArgs* connect = nullptr;
        TaskQueryArgs* query = nullptr;
        TaskExecuteArgs* exec = nullptr;
        switch (this->func) {
            case TAOS_FUNC::CONNECT:
                connect = static_cast<TaskConnectArgs *>(this->args);
                delete connect;
                break;
            case TAOS_FUNC::SELECT_DB:
            case TAOS_FUNC::QUERY:
            case TAOS_FUNC::QUERY_A:
            case TAOS_FUNC::STMT_PREPARE:
                query = static_cast<TaskQueryArgs *>(this->args);
                delete query;
                break;
            case TAOS_FUNC::STMT_EXECUTE:
                exec = static_cast<TaskExecuteArgs *>(this->args);
                delete exec;
                break;
            default:
                break;
        }
        this->args = nullptr;
    }
}

TaskConnectArgs::TaskConnectArgs(std::string &_host, int &_port, std::string &_user, std::string &_passwd, int &_is_hash,
                                 std::string &_database) {
    this->host = _host;
    this->port = _port;
    this->user = _user;
    this->passwd = _passwd;
    this->is_hash = _is_hash;
    this->database = _database;
}

TaskConnectArgs *TaskConnectArgs::parse(ErlNifEnv* env, const ERL_NIF_TERM raw) {
    std::tuple<std::string, int, std::string,std::string, int, std::string> args_tup;
    try {
        nifpp::get_throws(env, raw, args_tup);
        std::string &host = std::get<0>(args_tup);
        int &port = std::get<1>(args_tup);
        std::string &user = std::get<2>(args_tup);
        std::string &passwd = std::get<3>(args_tup);
        int &is_hash = std::get<4>(args_tup);
        std::string &database = std::get<5>(args_tup);
        return new TaskConnectArgs(host, port, user, passwd, is_hash, database);
    } catch (...) {
        return nullptr;
    }
}

TaskQueryArgs::TaskQueryArgs(std::string &_sql) {
    this->sql = _sql;
}

TaskQueryArgs *TaskQueryArgs::parse(ErlNifEnv *env, ERL_NIF_TERM raw) {
    try {
        std::string sql;
        nifpp::get_throws(env, raw, sql);
        return new TaskQueryArgs(sql);
    } catch (...) {
        return nullptr;
    }
}

TaskExecuteArgs::TaskExecuteArgs(int &_num_row, std::vector<int> &_types,
                                 std::vector<std::tuple<int, std::vector<int32_t>, ErlNifBinary>>& _params) {
    this->num_row = _num_row;
    this->types = _types;
    this->params.clear();
    for(auto it: _params) {
        auto t1 = std::get<0>(it);
        auto t2 = std::get<1>(it);
        nifpp::vec_bin t3(std::get<2>(it));
        this->params.emplace_back(std::tie(t1, t2, t3));
    }
//    printf("num_row = %d\n", num_row);
//    printf("type = ");
//    for(auto it: types) {
//        printf("%d ", it);
//    }
//    printf("\n");
//    printf("params = ");
//    printf("element size = %d", std::get<0>(params[0]));
//    printf("len size = %zu", std::get<1>(params[0]).size());
//    printf("\n");
}

TaskExecuteArgs *TaskExecuteArgs::parse(ErlNifEnv *env, ERL_NIF_TERM raw) {
    try {
        std::tuple<int, std::vector<int>, std::vector<std::tuple<int, std::vector<int32_t>, ErlNifBinary>>> args_tup;
        nifpp::get_throws(env, raw, args_tup);
        return new TaskExecuteArgs(std::get<0>(args_tup), std::get<1>(args_tup), std::get<2>(args_tup));
    } catch (...) {
        return nullptr;
    }
}
