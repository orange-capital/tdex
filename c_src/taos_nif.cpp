//
// Created by noname on 23/09/2024.
//
#include "taos.h"
#include "nifpp.h"

extern "C" {


static int init_nif(ErlNifEnv *env, void **priv_data, __attribute__((unused)) ERL_NIF_TERM load_info) {
    taos_init();
    return 0;
}

static void destroy_nif(__attribute__((unused)) ErlNifEnv *env, void *priv_data) {
    if (priv_data) {
        enif_free(priv_data);
    }
    taos_cleanup();
}

static ErlNifFunc nif_funcs[] = {
//        {"taos_connect_nif",                  6, taos_connect_nif, 0},
//        {"taos_close",                    1,     taos_close_nif,           0},
};

ERL_NIF_INIT(Elixir.TDex.Wrapper, nif_funcs, init_nif, nullptr, nullptr, destroy_nif)
}