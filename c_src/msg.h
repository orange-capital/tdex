//
// Created by noname on 23/09/2024.
//
#ifndef TAOS_C_SRC_MSG_H
#define TAOS_C_SRC_MSG_H

#include "nifpp.h"


enum class TAOS_FUNC {
    CONNECT = 0,
    CLOSE,
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

struct ErlTask {
public:
    ErlNifPid cb_pid;
    uint64_t cb_id;
    int conn;
    int stmt;
    TAOS_FUNC func;
    nifpp::TERM args;

    explicit ErlTask(ErlNifPid cb_pid, uint64_t cb_id, int conn, int stmt, TAOS_FUNC func, nifpp::TERM args);
    ~ErlTask() = default;

    static ErlTask* create(ErlNifEnv* env, const ERL_NIF_TERM* argv);
};

#endif //TAOS_C_SRC_MSG_H
