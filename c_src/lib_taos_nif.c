#include <erl_nif.h>
#include <stdlib.h>
#include <string.h>
#include <taos.h>

#define MAX_SQL_SIZE 1048576
#define DB_NAME_MAX_LEN 256
#define VARSTR_HEADER_SIZE sizeof(uint16_t)

static ErlNifResourceType *TAOS_TYPE;
static ErlNifResourceType *TAOS_STMT_TYPE;

static ERL_NIF_TERM atom_ok;
static ERL_NIF_TERM atom_error;
static ERL_NIF_TERM atom_invalid_resource;
static ERL_NIF_TERM atom_sql_too_big;
static ERL_NIF_TERM atom_invalid_callback_args;
static ERL_NIF_TERM atom_out_of_memory;
static ERL_NIF_TERM atom_over_size;
static ERL_NIF_TERM atom_nil;
static ERL_NIF_TERM atom_true;
static ERL_NIF_TERM atom_false;
static ERL_NIF_TERM atom_json;
static ERL_NIF_TERM atom_ts;

typedef struct {
    TAOS *taos;
} WRAP_TAOS;

typedef struct {
    TAOS_STMT *stmt;
    TAOS_MULTI_BIND *params;
    unsigned int param_count;
} WRAP_TAOS_STMT;

typedef struct {
    uint32_t taos_type;
    uint32_t taos_res_type;
    uint32_t taos_stmt_type;
} resource_usage_t;

typedef struct {
    uint64_t id;
    uint32_t total_row;
    ErlNifPid pid;
    ErlNifEnv* env;
} CB_DATA_T;


static inline ERL_NIF_TERM make_taos_error_tuple(ErlNifEnv* env, TAOS_RES* res) {
    const char* error_str = taos_errstr(res);
    ErlNifBinary error_bin;
    if (!enif_alloc_binary(strlen(error_str), &error_bin)) {
        return enif_make_tuple2(env, atom_error, atom_out_of_memory);
    }
    memcpy(error_bin.data, error_str, error_bin.size);
    return enif_make_tuple2(env, atom_error, enif_make_binary(env, &error_bin));
}

/* STMT API */

static ERL_NIF_TERM taos_stmt_prepare_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    if (argc != 2) {
        return enif_make_badarg(env);
    }

    WRAP_TAOS *taos_obj = NULL;
    if (!enif_get_resource(env, argv[0], TAOS_TYPE, (void **) &taos_obj)) {
        return enif_make_tuple2(env, atom_error, atom_invalid_resource);
    }

    unsigned sql_length;
    enif_get_list_length(env, argv[1], &sql_length);
    char sql[sql_length + 1];
    if (!enif_get_string(env, argv[1], sql, sizeof(sql), ERL_NIF_LATIN1)) {
        return enif_make_badarg(env);
    }

    unsigned int param_count = 0;
    for (int i = 0; i < sql_length; i++) {
        if (sql[i] == '?') param_count++;
    }

    TAOS_STMT *stmt = taos_stmt_init(taos_obj->taos);
    int code = taos_stmt_prepare(stmt, sql, 0);
    if (code) {
        taos_stmt_close(stmt);
        return enif_make_tuple2(env, atom_error, enif_make_int(env, code));
    }
    TAOS_MULTI_BIND *params = NULL;
    if (param_count > 0) {
        params = (TAOS_MULTI_BIND *) malloc(sizeof(TAOS_MULTI_BIND) * param_count);
        if (params == NULL) {
            taos_stmt_close(stmt);
            return enif_make_tuple2(env, atom_error, atom_out_of_memory);
        }
    }
    WRAP_TAOS_STMT *stmt_ptr = (WRAP_TAOS_STMT *) enif_alloc_resource(TAOS_STMT_TYPE, sizeof(WRAP_TAOS_STMT));
    stmt_ptr->stmt = stmt;
    stmt_ptr->params = params;
    stmt_ptr->param_count = param_count;
    ERL_NIF_TERM result = enif_make_resource(env, stmt_ptr);
    enif_release_resource(stmt_ptr);
    return enif_make_tuple2(env, atom_ok, result);
}

static ERL_NIF_TERM taos_stmt_execute_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    if (argc != 2) {
        return enif_make_badarg(env);
    }
}

static ERL_NIF_TERM taos_stmt_execute_a_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    if (argc != 4) {
        return enif_make_badarg(env);
    }
    return atom_ok;
}

static ERL_NIF_TERM taos_stmt_free_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    if (argc != 1) {
        return enif_make_badarg(env);
    }
    WRAP_TAOS_STMT *stmt_ptr = NULL;
    if (!enif_get_resource(env, argv[0], TAOS_STMT_TYPE, (void **) &stmt_ptr)) {
        return enif_make_tuple2(env, atom_error, atom_invalid_resource);
    }
    taos_stmt_close(stmt_ptr->stmt);
    return atom_ok;
}

/* BASIC API TAOS */
static ERL_NIF_TERM taos_connect_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    if (argc != 6) {
        return enif_make_badarg(env);
    }

    WRAP_TAOS *taos_obj = NULL;
    TAOS* taos = NULL;
    ErlNifBinary ip, user, pass, db;
    unsigned int port = 0, is_md5 = 0;

    if (!enif_inspect_binary(env, argv[0], &ip)) {
        return enif_make_badarg(env);
    }
    if (!enif_get_uint(env, argv[1], &port)) {
        return enif_make_badarg(env);
    }
    if (!enif_inspect_binary(env, argv[2], &user)) {
        return enif_make_badarg(env);
    }

    if (!enif_inspect_binary(env, argv[3], &pass)) {
        return enif_make_badarg(env);
    }
    if (!enif_get_uint(env, argv[4], &is_md5)) {
        return enif_make_badarg(env);
    }
    if (!enif_inspect_binary(env, argv[5], &db)) {
        return enif_make_badarg(env);
    }
    if (is_md5 == 0) {
        taos = taos_connect((const char *)ip.data, (const char *)user.data, (const char *)pass.data, (const char *)db.data, port);
    } else {
        taos = taos_connect_auth((const char *)ip.data, (const char *)user.data, (const char *)pass.data, (const char *)db.data, port);
    }
    if (taos == NULL) {
        return make_taos_error_tuple(env, NULL);
    }
    taos_obj = (WRAP_TAOS *) enif_alloc_resource(TAOS_TYPE, sizeof(WRAP_TAOS));
    taos_obj->taos = taos;
    ERL_NIF_TERM connect = enif_make_resource(env, taos_obj);
    enif_release_resource(taos_obj);
    return enif_make_tuple2(env, atom_ok, connect);
}

static ERL_NIF_TERM taos_close_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    if (argc != 1) {
        return enif_make_badarg(env);
    }
    WRAP_TAOS *taos_obj = NULL;
    if (!enif_get_resource(env, argv[0], TAOS_TYPE, (void **) &taos_obj)) {
        return enif_make_tuple2(env, atom_error, atom_invalid_resource);
    }
    taos_kill_query(taos_obj->taos);
    taos_close(taos_obj->taos);
    enif_release_resource(taos_obj);
    return atom_ok;
}

static ERL_NIF_TERM taos_options_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    if (argc != 2) {
        return enif_make_badarg(env);
    }
    int option = 0;
    ErlNifBinary option_value;
    if (!enif_get_int(env, argv[0], &option) || option < TSDB_OPTION_LOCALE || option > TSDB_MAX_OPTIONS) {
        return enif_make_badarg(env);
    }
    if (!enif_inspect_binary(env, argv[1], &option_value)) {
        return enif_make_badarg(env);
    }
    taos_options(option, option_value.data);
    return atom_ok;
}

static ERL_NIF_TERM taos_select_db_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    if (argc != 2) {
        return enif_make_badarg(env);
    }

    WRAP_TAOS *taos_obj = NULL;
    ErlNifBinary db;

    if (!enif_get_resource(env, argv[0], TAOS_TYPE, (void **) &taos_obj)) {
        return enif_make_tuple2(env, atom_error, atom_invalid_resource);
    }

    if (!enif_inspect_binary(env, argv[1], &db)) {
        return enif_make_badarg(env);
    }

    int res = taos_select_db(taos_obj->taos, (const char *)db.data);
    if (res != 0) {
        return make_taos_error_tuple(env, NULL);
    }
    return atom_ok;
}

static ERL_NIF_TERM taos_get_current_db_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    if (argc != 1) {
        return enif_make_badarg(env);
    }
    WRAP_TAOS *taos_obj = NULL;
    if (!enif_get_resource(env, argv[0], TAOS_TYPE, (void **) &taos_obj)) {
        return enif_make_tuple2(env, atom_error, atom_invalid_resource);
    }
    char db_name[DB_NAME_MAX_LEN];
    int required_len = 0;
    memset(db_name, 0, DB_NAME_MAX_LEN);
    if (taos_get_current_db(taos_obj->taos, db_name, DB_NAME_MAX_LEN, &required_len) != 0) {
        return enif_make_tuple2(env, atom_error, atom_over_size);
    }
    ErlNifBinary db_bin;
    if(!enif_alloc_binary(strlen(db_name), &db_bin)) {
        return enif_make_tuple2(env, atom_error, atom_out_of_memory);
    }
    memcpy(db_bin.data, db_name, db_bin.size);
    return enif_make_tuple2(env, atom_ok, enif_make_binary(env, &db_bin));
}


static ERL_NIF_TERM taos_get_client_info_nif(ErlNifEnv* env, __attribute__((unused)) int argc,
                                             __attribute__((unused)) const ERL_NIF_TERM argv[]) {
    ErlNifBinary ret;
    const char *client_info = taos_get_client_info();
    size_t client_info_size = strlen(client_info);
    if(!enif_alloc_binary(client_info_size, &ret)) {
        return enif_make_tuple2(env, atom_error, atom_out_of_memory);
    }
    memcpy(ret.data, client_info, ret.size);
    return enif_make_binary(env, &ret);
}

static ERL_NIF_TERM taos_get_server_info_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    if (argc != 1) {
        return enif_make_badarg(env);
    }
    WRAP_TAOS *taos_obj = NULL;
    if (!enif_get_resource(env, argv[0], TAOS_TYPE, (void **) &taos_obj)) {
        return enif_make_tuple2(env, atom_error, atom_invalid_resource);
    }
    ErlNifBinary ret;
    const char* server_info = taos_get_server_info(taos_obj->taos);
    size_t server_info_size = strlen(server_info);
    if(!enif_alloc_binary(server_info_size, &ret)) {
        return enif_make_tuple2(env, atom_error, atom_out_of_memory);
    }
    memcpy(ret.data, server_info, ret.size);
    return enif_make_binary(env, &ret);
}

/* Free query result */
static inline int parse_row_data(ErlNifEnv *env, ERL_NIF_TERM *row_data_array, int field_count, TAOS_FIELD* fields, TAOS_ROW row) {
    int8_t int8_val = 0;
    uint8_t uint8_val = 0;
    int16_t int16_val = 0;
    uint16_t uint16_val = 0;
    int32_t int32_val = 0;
    uint32_t uint32_val = 0;
    int64_t int64_val = 0;
    uint64_t uint64_val = 0;
    float float_val = 0;
    double double_val = 0;
    ErlNifBinary bin;
    for(int i = 0; i < field_count; i++) {
        switch (fields[i].type) {
            case TSDB_DATA_TYPE_NULL:
                row_data_array[i] = atom_nil;
                break;
            case TSDB_DATA_TYPE_BOOL:
                int8_val = *((int8_t *)row[i]);
                if (int8_val) {
                    row_data_array[i] = atom_true;
                } else {
                    row_data_array[i] = atom_false;
                }
                break;
            case TSDB_DATA_TYPE_TINYINT:
                int8_val = *((int8_t *)row[i]);
                row_data_array[i] = enif_make_int(env, int8_val);
                break;
            case TSDB_DATA_TYPE_UTINYINT:
                uint8_val = *((uint8_t *)row[i]);
                row_data_array[i] = enif_make_uint(env, uint8_val);
                break;
            case TSDB_DATA_TYPE_SMALLINT:
                int16_val = *((int16_t *)row[i]);
                row_data_array[i] = enif_make_int(env, int16_val);
                break;
            case TSDB_DATA_TYPE_USMALLINT:
                uint16_val = *((uint16_t *)row[i]);
                row_data_array[i] = enif_make_uint(env, uint16_val);
                break;
            case TSDB_DATA_TYPE_INT:
                int32_val = *((int32_t *)row[i]);
                row_data_array[i] = enif_make_int(env, int32_val);
                break;
            case TSDB_DATA_TYPE_UINT:
                uint32_val = *((uint32_t *)row[i]);
                row_data_array[i] = enif_make_uint(env, uint32_val);
                break;
            case TSDB_DATA_TYPE_BIGINT:
                int64_val = *((int64_t *)row[i]);
                row_data_array[i] = enif_make_int64(env, int64_val);
                break;
            case TSDB_DATA_TYPE_UBIGINT:
                uint64_val = *((uint64_t *)row[i]);
                row_data_array[i] = enif_make_uint64(env, uint64_val);
                break;
            case TSDB_DATA_TYPE_FLOAT:
                float_val = *((float *)row[i]);
                row_data_array[i] = enif_make_double(env, float_val);
                break;
            case TSDB_DATA_TYPE_DOUBLE:
                double_val = *((double *)row[i]);
                row_data_array[i] = enif_make_double(env, double_val);
                break;
            case TSDB_DATA_TYPE_TIMESTAMP:
                int64_val = *((int64_t *)row[i]);
                row_data_array[i] = enif_make_tuple2(env, atom_ts, enif_make_int64(env, int64_val));
                break;
            case TSDB_DATA_TYPE_JSON:
                row_data_array[i] = atom_json;
                break;

            case TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_NCHAR:
            case TSDB_DATA_TYPE_VARBINARY:
            case TSDB_DATA_TYPE_BLOB:
            case TSDB_DATA_TYPE_MEDIUMBLOB:
            case TSDB_DATA_TYPE_GEOMETRY:
                uint16_val = *((uint16_t*)((uint8_t*)row[i] - VARSTR_HEADER_SIZE));
                if(!enif_alloc_binary(uint16_val, &bin)) {
                    return 0;
                }
                memcpy(bin.data, row[i], bin.size);
                row_data_array[i] = enif_make_binary(env, &bin);
                break;
            default:
                break;
        }
    }
    return 1;
}

static inline int parse_taos_res(ErlNifEnv *env, ERL_NIF_TERM* headers, ERL_NIF_TERM* data, TAOS_RES* res) {
    int field_count = taos_field_count(res);
    TAOS_FIELD* fields = taos_fetch_fields(res);
    ERL_NIF_TERM *array_ret = enif_alloc(sizeof(ERL_NIF_TERM) * field_count);
    if (array_ret == NULL) {
        return 0;
    }
    for(int i = 0; i < field_count; i++) {
        array_ret[i] = enif_make_tuple2(env, enif_make_string(env, fields[i].name, ERL_NIF_UTF8), enif_make_uint(env, fields[i].type));
    }
    *headers = enif_make_tuple_from_array(env, array_ret, field_count);
    enif_free(array_ret);
    *data = enif_make_list(env, 0);
    TAOS_ROW row = NULL;
    while ((row = taos_fetch_row(res))) {
        ERL_NIF_TERM *row_data_array = enif_alloc(sizeof(ERL_NIF_TERM) * field_count);
        if (row_data_array == NULL) {
            return 0;
        }
        if (!parse_row_data(env, row_data_array, field_count, fields, row)) {
            return 0;
        }
        ERL_NIF_TERM row_data = enif_make_tuple_from_array(env, row_data_array, field_count);
        enif_free(row_data_array);
        *data = enif_make_list_cell(env, row_data, *data);
    }
    taos_free_result(res);
    return 1;
}


/* Synchronous APIs */
static ERL_NIF_TERM taos_query_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    if (argc != 2) {
        return enif_make_badarg(env);
    }

    ErlNifBinary sql_query;
    WRAP_TAOS *taos_obj = NULL;
    TAOS_RES *taos_result = NULL;
    int error_code = 0;


    if (!enif_get_resource(env, argv[0], TAOS_TYPE, (void **) &taos_obj)) {
        return enif_make_tuple2(env, atom_error, atom_invalid_resource);
    }
    if (!enif_inspect_binary(env, argv[1], &sql_query)) {
        return enif_make_badarg(env);
    }
    if (sql_query.size > MAX_SQL_SIZE) {
        return enif_make_tuple2(env, atom_error, atom_sql_too_big);
    }
    taos_result = taos_query(taos_obj->taos, (const char*)sql_query.data);
    error_code = taos_errno(taos_result);
    if (error_code != 0) {
        ErlNifBinary error_bin;
        const char* error_msg = taos_errstr(taos_result);
        if(!enif_alloc_binary(strlen(error_msg), &error_bin)) {
            return enif_make_tuple2(env, atom_error, atom_out_of_memory);
        }
        memcpy(error_bin.data, error_msg, error_bin.size);
        return enif_make_tuple2(env, atom_error, enif_make_binary(env, &error_bin));
    }
    int precision = taos_result_precision(taos_result);
    int affected_rows = taos_affected_rows(taos_result);
    ERL_NIF_TERM row_header, row_data;
    if (!parse_taos_res(env, &row_header, &row_data, taos_result)) {
        return enif_make_tuple2(env, atom_error, atom_out_of_memory);
    }
    ERL_NIF_TERM metadata = enif_make_tuple3(env, enif_make_uint(env, precision),
                                             enif_make_uint(env, affected_rows),
                                             row_header);
    return enif_make_tuple3(env, atom_ok, metadata, row_data);
}

/* Asynchronous APIs */
void taos_fetch_rows_a_cb(void* params, TAOS_RES* res, int num_of_row);

static inline void cb_param_free(CB_DATA_T* cb) {
    enif_free_env(cb->env);
    enif_free(cb);
}

static inline void send_error_to_cb(CB_DATA_T* cb, const char* error_msg) {
    ERL_NIF_TERM pi_atom_error = enif_make_atom(cb->env, "error");
    ERL_NIF_TERM pi_atom_oom = enif_make_atom(cb->env, "out_of_memory");
    ERL_NIF_TERM pi_atom_reply = enif_make_atom(cb->env, "taos_reply");
    ERL_NIF_TERM reply_msg, reply;
    ErlNifBinary error_bin;
    if(!enif_alloc_binary(strlen(error_msg), &error_bin)) {
        reply_msg = enif_make_tuple2(cb->env, pi_atom_error, pi_atom_oom);
    } else {
        memcpy(error_bin.data, error_msg, error_bin.size);
        reply_msg = enif_make_tuple2(cb->env, pi_atom_error, enif_make_binary(cb->env, &error_bin));
    }
    reply = enif_make_tuple3(cb->env, pi_atom_reply, enif_make_uint64(cb->env, cb->id), reply_msg);
    enif_send(NULL, &cb->pid, cb->env, reply);
    cb_param_free(cb);
}

static inline int parse_taos_res_a(CB_DATA_T* cb_data, TAOS_RES* res, int number_of_row) {
    ERL_NIF_TERM pi_atom_reply = enif_make_atom(cb_data->env, "taos_reply");
    ERL_NIF_TERM pi_atom_ok = enif_make_atom(cb_data->env, "ok");
    ERL_NIF_TERM reply, metadata, row_data, row_header;
    TAOS_FIELD* fields = taos_fetch_fields(res);
    int field_count = taos_field_count(res);
    if (cb_data->total_row == 0) {
        int precision = taos_result_precision(res);
        int affected_rows = taos_affected_rows(res);
        ERL_NIF_TERM* header_array = enif_alloc(sizeof(ERL_NIF_TERM) * field_count);
        if (header_array == NULL) {
            return 0;
        }
        for(int i = 0; i < field_count; i++) {
            header_array[i] = enif_make_tuple2(cb_data->env,
                                           enif_make_string(cb_data->env, fields[i].name, ERL_NIF_UTF8),
                                           enif_make_uint(cb_data->env, fields[i].type));
        }
        row_header = enif_make_tuple_from_array(cb_data->env, header_array, field_count);
        metadata = enif_make_tuple3(cb_data->env,
                                    enif_make_uint(cb_data->env, precision),
                                    enif_make_uint(cb_data->env, affected_rows),
                                    row_header);
    } else {
        metadata = enif_make_atom(cb_data->env, "nil");
    }
    row_data = enif_make_list(cb_data->env, 0);
    for (int i = 0; i < number_of_row; i++) {
        TAOS_ROW row = taos_fetch_row(res);
        ERL_NIF_TERM *data_array = enif_alloc(sizeof(ERL_NIF_TERM) * field_count);
        if (data_array == NULL) {
            return 0;
        }
        if (!parse_row_data(cb_data->env, data_array, field_count, fields, row)) {
            enif_free(data_array);
            return 0;
        }
        row_data = enif_make_list_cell(cb_data->env, enif_make_tuple_from_array(cb_data->env, data_array, field_count), row_data);
    }
    reply = enif_make_tuple3(cb_data->env, pi_atom_reply, enif_make_uint64(cb_data->env, cb_data->id), enif_make_tuple3(cb_data->env, pi_atom_ok, metadata, row_data));
    enif_send(NULL, &cb_data->pid, cb_data->env, reply);
    cb_data->total_row += number_of_row;
    enif_clear_env(cb_data->env);
    taos_fetch_rows_a(res, taos_fetch_rows_a_cb, cb_data);
    return 1;
}


void taos_fetch_rows_a_cb(void* params, TAOS_RES* res, int num_of_row) {
    CB_DATA_T* cb = (CB_DATA_T*)params;
    ERL_NIF_TERM reply;
    if (num_of_row > 0) {
        parse_taos_res_a(cb, res, num_of_row);
    } else {
        ERL_NIF_TERM pi_atom_reply = enif_make_atom(cb->env, "taos_reply");
        reply = enif_make_tuple2(cb->env, pi_atom_reply, enif_make_uint64(cb->env, cb->id));
        enif_send(NULL, &cb->pid, cb->env, reply);
        cb_param_free(cb);
    }
}

void taos_query_a_cb(void* params, TAOS_RES* res, int error_code) {
    CB_DATA_T* async_param = (CB_DATA_T*)params;
    if (error_code != 0) {
        const char* error_msg = taos_errstr(res);
        send_error_to_cb(async_param, error_msg);
        return;
    }
    taos_fetch_rows_a(res, taos_fetch_rows_a_cb, params);
}

static ERL_NIF_TERM taos_query_a_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    if (argc != 4) {
        return enif_make_badarg(env);
    }
    ErlNifBinary sql_query;
    CB_DATA_T* cb_data;
    WRAP_TAOS *taos_obj = NULL;
    if (!enif_get_resource(env, argv[0], TAOS_TYPE, (void **) &taos_obj)) {
        return enif_make_tuple2(env, atom_error, atom_invalid_resource);
    }
    if (!enif_inspect_binary(env, argv[1], &sql_query)) {
        return enif_make_badarg(env);
    }
    if (sql_query.size > MAX_SQL_SIZE) {
        return enif_make_tuple2(env, atom_error, atom_sql_too_big);
    }
    cb_data = (CB_DATA_T*)enif_alloc(sizeof(CB_DATA_T));
    if (cb_data == NULL) {
        return enif_make_tuple2(env, atom_error, atom_out_of_memory);
    }
    cb_data->total_row = 0;
    cb_data->env = enif_alloc_env();
    if (cb_data->env == NULL) {
        enif_free(cb_data);
        return enif_make_tuple2(env, atom_error, atom_out_of_memory);
    }
    if (!enif_get_local_pid(env, argv[2], &cb_data->pid)) {
        enif_free(cb_data);
        return enif_make_tuple2(env, atom_error, atom_invalid_callback_args);
    }
    if (!enif_get_uint64(env, argv[3], &cb_data->id)) {
        enif_free(cb_data);
        return enif_make_tuple2(env, atom_error, atom_invalid_callback_args);
    }
    taos_query_a(taos_obj->taos, (const char*)sql_query.data, taos_query_a_cb, cb_data);
    return atom_ok;
}


/* Init NIF, resource ... */
static void free_taos_resource(__attribute__((unused)) ErlNifEnv *env, void *obj) {
    resource_usage_t* usage = (resource_usage_t*) obj;
    usage->taos_type--;
}

static void free_taos_stmt_resource(__attribute__((unused)) ErlNifEnv *env, void *obj) {
    resource_usage_t* usage = (resource_usage_t*) obj;
    usage->taos_stmt_type--;
}

static inline int init_taos_resource(ErlNifEnv *env) {
    const char *mod_taos = "TDEX";
    const char *name_taos = "TAOS_TYPE";
    const char *name_stmt_type = "TAOS_STMT_TYPE";
    int flags = ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER;

    TAOS_TYPE = enif_open_resource_type(env, mod_taos, name_taos, free_taos_resource, (ErlNifResourceFlags) flags,
                                        NULL);
    if (TAOS_TYPE == NULL) return -1;

    TAOS_STMT_TYPE = enif_open_resource_type(env, mod_taos, name_stmt_type, free_taos_stmt_resource,
                                             (ErlNifResourceFlags) flags, NULL);
    if (TAOS_STMT_TYPE == NULL) return -1;

    return 0;
}

static int init_nif(ErlNifEnv *env, void **priv_data, __attribute__((unused)) ERL_NIF_TERM load_info) {
    taos_init();
    if (init_taos_resource(env) == -1) {
        return -1;
    }
    // init private data
    resource_usage_t *usage = (resource_usage_t*) enif_alloc(sizeof(resource_usage_t));
    memset(usage, 0, sizeof(resource_usage_t));
    *priv_data = (void*) usage;
    // init common atom
    atom_ok = enif_make_atom(env, "ok");
    atom_error = enif_make_atom(env, "error");
    atom_invalid_resource = enif_make_atom(env, "invalid_resource");
    atom_sql_too_big = enif_make_atom(env, "sql_too_big");
    atom_out_of_memory = enif_make_atom(env, "out_of_memory ");
    atom_invalid_callback_args = enif_make_atom(env, "invalid_callback_args");
    atom_over_size = enif_make_atom(env, "over_size");
    atom_nil = enif_make_atom(env, "nil");
    atom_true = enif_make_atom(env, "true");
    atom_false = enif_make_atom(env, "false");
    atom_json = enif_make_atom(env, "json");
    atom_ts = enif_make_atom(env, "ts");
    return 0;
}

static void destroy_nif(__attribute__((unused)) ErlNifEnv *env, void *priv_data) {
    if (priv_data) {
        enif_free(priv_data);
    }
    taos_cleanup();
}

static ErlNifFunc nif_funcs[] = {
        {"taos_connect_nif",                  6, taos_connect_nif},
        {"taos_close",                    1,     taos_close_nif},
        {"taos_options_nif",                 2,      taos_options_nif},
        {"taos_get_current_db",          1,      taos_get_current_db_nif},
        {"taos_get_server_info",         1,      taos_get_server_info_nif},
        {"taos_get_client_info",          0,     taos_get_client_info_nif},
        {"taos_select_db_nif",                2,     taos_select_db_nif},
        {"taos_query_nif",               2,     taos_query_nif},
        {"taos_query_a_nif",             4,     taos_query_a_nif},
        {"taos_stmt_free",             4,     taos_stmt_free_nif},
        {"taos_stmt_prepare",             2,     taos_stmt_prepare_nif},
        {"taos_stmt_execute",             2,     taos_stmt_execute_nif},
        {"taos_stmt_execute_a",             4,     taos_stmt_execute_a_nif},
};

ERL_NIF_INIT(Elixir.TDex.Wrapper, nif_funcs, init_nif, NULL, NULL, destroy_nif)