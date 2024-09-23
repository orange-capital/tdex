//
// Created by noname on 23/09/2024.
//
#include "msg.h"

ErlTask::ErlTask(ErlNifPid _cb_pid, uint64_t cb_id, int conn, int stmt, TAOS_FUNC func, nifpp::TERM args): cb_pid(_cb_pid) {
    this->cb_id = cb_id;
    this->conn = conn;
    this->stmt = stmt;
    this->func = func;
    this->args = args;
}

ErlTask *ErlTask::create(ErlNifEnv* env, const ERL_NIF_TERM *argv) {
    try {
        ErlNifPid cb_pid;
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
        return new ErlTask(cb_pid, cb_id, conn, stmt, static_cast<TAOS_FUNC>(func), nifpp::TERM(argv[5]));
    } catch (...) {
        return nullptr;
    }
}

