//
// Created by noname on 23/09/2024.
//
#include "taos.h"
#include "worker.h"

extern "C" {

static ERL_NIF_TERM atom_ok;
static ERL_NIF_TERM atom_error;
static ERL_NIF_TERM atom_rt_error;

static ERL_NIF_TERM call_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    if (argc != 6) {
        return enif_make_tuple2(env, atom_error, enif_make_atom(env, "invalid_arity"));
    }
    ErlTask* task = ErlTask::create(env, argv);
    if (task == nullptr) {
        return enif_make_tuple2(env, atom_error, enif_make_atom(env, "invalid_arg"));
    }
    if (WorkerManager::instance()->_worker_count == 0) {
        return enif_make_tuple2(env, atom_error, enif_make_atom(env, "no_worker"));
    }
    if(!WorkerManager::instance()->dispatch(task)) {
        return enif_make_tuple2(env, atom_error, enif_make_atom(env, "route_fail"));
    }
    return atom_ok;
}


static ERL_NIF_TERM start_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    if (argc != 1) {
        return enif_make_badarg(env);
    }
    int num_worker = 0;
    if (!enif_get_int(env, argv[0], &num_worker)) {
        return enif_make_tuple2(env, atom_error, enif_make_atom(env, "invalid_arg"));
    }
    if (WorkerManager::instance()->start(num_worker)) {
        return atom_ok;
    } else {
        return enif_make_tuple2(env, atom_error, atom_rt_error);
    }
}

static ERL_NIF_TERM stop_nif([[maybe_unused]] ErlNifEnv *env, [[maybe_unused]] int argc, [[maybe_unused]] const ERL_NIF_TERM argv[]) {
    WorkerManager::instance()->stop();
    WorkerManager::dispose();
    taos_cleanup();
    return atom_ok;
}


static int init_nif(ErlNifEnv *env, [[maybe_unused]] void **priv_data, [[maybe_unused]] ERL_NIF_TERM load_info) {
    taos_init();
    *priv_data = nullptr;
    atom_ok = enif_make_atom(env, "ok");
    atom_error = enif_make_atom(env, "error");
    atom_rt_error = enif_make_atom(env, "rt_error");
    WorkerManager::instance();
    return 0;
}

static ErlNifFunc nif_funcs[] = {
        {"call_nif", 6, call_nif, 0},
        {"start_nif",                  1, start_nif, 0},
        {"stop_nif", 0, stop_nif, 0},
};

ERL_NIF_INIT(Elixir.TDex.Wrapper, nif_funcs, init_nif, nullptr, nullptr, nullptr)
}