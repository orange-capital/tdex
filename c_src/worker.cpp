//
// Created by noname on 23/09/2024.
//
#include "worker.h"
#include <thread>
#include <chrono>
#include <iostream>

WorkerManager* WorkerManager::_instance = nullptr;
std::mutex WorkerManager::_instance_mut;

WorkerManager *WorkerManager::instance() {
    _instance_mut.lock();
    if (_instance == nullptr) {
        _instance = new WorkerManager();
    }
    _instance_mut.unlock();
    return _instance;
}

void WorkerManager::dispose() {
    _instance_mut.lock();
    delete _instance;
    _instance = nullptr;
    _instance_mut.unlock();
}

WorkerManager::WorkerManager() : _route_ptr(0) , _worker_count(0){
    this->_workers.clear();
}

WorkerManager::~WorkerManager() {
    this->stop();
}

int WorkerManager::start(int num) {
    if (this->_worker_count > 0 || num <= 0) {
        return 0;
    }
    for (int i = 0; i < num; i++) {
        auto* worker = new Worker(i);
        worker->start();
        this->_workers.push_back(worker);
    }
    this->_worker_count = num;
    return 1;
}

void WorkerManager::stop() {
    for(auto & _worker : this->_workers) {
        _worker->stop();
        delete _worker;
    }
    this->_workers.clear();
    this->_worker_count = 0;
}

bool WorkerManager::dispatch(ErlTask *task) {
    int route_id = 0;
    if (task->conn < 0) {
        if (task->func == TAOS_FUNC::GET_CLIENT_INFO) {
            route_id = 0;
        } else if (task->func != TAOS_FUNC::CONNECT) {
            return false;
        } else {
            route_id = (int)_route_ptr;
            _route_ptr++;
            if (_route_ptr > INT32_MAX) {
                _route_ptr = 0;
            }
        }
    } else {
        route_id = (task->conn >> 16) & 0x03FF;
    }
    int slot_id = route_id % (int)this->_worker_count;
    this->_workers[slot_id]->send(task);
    return true;
}

void Worker::stop() {
    if(!_is_stop) {
        _is_stop = true;
        enif_thread_join(_tid, nullptr);
    }
}

Worker::Worker(int id) : _is_stop(true), _tid(nullptr), _env(nullptr) {
    _taos_conn_ptr = 0;
    _taos_stmt_ptr = 0;
    _id = id;
    _env = enif_alloc_env();
    assert(_env != nullptr);
    _taos_conn.clear();
    _taos_stmt.clear();
}

Worker::~Worker() {
    this->stop();
    enif_free_env(_env);
    // release pending task
    _queue_mut.lock();
    ErlTask* task;
    while (true) {
        if(_queue.empty()) {
            break;
        }
        task = _queue.front();
        _queue.pop();
        delete task;
    }
    _queue_mut.unlock();
    // release pending taos
    for (const auto &pair: _taos_stmt) {
        ::taos_stmt_close(pair.second);
    }
    for (auto &pair: _taos_conn) {
        ::taos_close(pair.second);
    }
    _taos_stmt.clear();
    _taos_conn.clear();
}

void* worker_run(void* args) {
    auto* obj = static_cast<Worker *>(args);
    obj->run();
    return nullptr;
}

int Worker::start() {
    char thread_name[128];
    memset(thread_name, 0, 128);
    sprintf(thread_name, "taos_nif_thread_%d", _id);
    if (!this->_is_stop) {
        return 1;
    }
    this->_is_stop = false;
    return enif_thread_create(thread_name, &_tid, &worker_run, this, nullptr);
}

void Worker::run() {
    int task_count = 0;
    while (!this->_is_stop) {
        task_count = this->process_batch();
        if (task_count > 0) {
            // clear env nif
            enif_clear_env(_env);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
}

void Worker::process(ErlTask* task) {
    switch (task->func) {
        case TAOS_FUNC::QUERY:
            this->taos_query(task);
            break;
        case TAOS_FUNC::QUERY_A:
            this->taos_query_a(task);
            break;
        case TAOS_FUNC::CONNECT:
            this->taos_connect(task);
            break;
        case TAOS_FUNC::CLOSE:
            this->taos_close(task);
            break;
        case TAOS_FUNC::GET_SERVER_INFO:
            this->taos_get_server_info(task);
            break;
        case TAOS_FUNC::GET_CLIENT_INFO:
            this->taos_get_client_info(task);
            break;
        case TAOS_FUNC::GET_CURRENT_DB:
            this->taos_get_current_db(task);
            break;
        case TAOS_FUNC::SELECT_DB:
            this->taos_select_db(task);
            break;
        default:
            this->reply_error_str(task, "not_implemented");
            break;
    }
}

void Worker::send(ErlTask *task) {
    _queue_mut.lock();
    _queue.push(task);
    _queue_mut.unlock();
}

int Worker::process_batch() {
    int ret = 0;
    ErlTask* task = nullptr;
    while (ret < TASK_BATCH_SIZE) {
        _queue_mut.lock();
        if (_queue.empty()) {
            _queue_mut.unlock();
            break;
        }
        task = _queue.front();
        _queue.pop();
        _queue_mut.unlock();
        this->process(task);
        delete task;
        ret++;
    }
    return ret;
}

inline void Worker::reply(ErlTask* task, nifpp::TERM term) {
    std::tuple<nifpp::str_atom, uint64_t, nifpp::TERM> reply_tup = std::tie("taos_reply", task->cb_id, term);
    enif_send(nullptr, &task->cb_pid, _env, nifpp::make(_env, reply_tup));
}

inline void Worker::reply_error(ErlTask *task, const char *reason) {
    std::tuple<nifpp::str_atom, nifpp::str_atom> error_tup = std::tie("error", reason);
    this->reply(task, nifpp::make(this->_env, error_tup));
}

inline void Worker::reply_error_str(ErlTask *pTask, const char *message) {
    nifpp::atom error(_env, "error");
    nifpp::binary reason(message);
    auto error_tup = std::tie(error, reason);
    this->reply(pTask, nifpp::make(this->_env, error_tup));
}

void Worker::taos_connect(ErlTask *task) {
    if (task->conn < 0) {
        auto* args = static_cast<TaskConnectArgs *>(task->args);
        if (args == nullptr) {
            this->reply_error(task, "invalid_arg");
            return;
        }
        TAOS* conn = nullptr;
        if (args->is_hash == 0) {
            conn = ::taos_connect(args->host.c_str(), args->user.c_str(), args->passwd.c_str(), args->database.c_str(), args->port);
        } else {
            conn = ::taos_connect_auth(args->host.c_str(), args->user.c_str(), args->passwd.c_str(), args->database.c_str(), args->port);
        }
        if (conn == nullptr) {
            this->reply_error_str(task, ::taos_errstr(nullptr));
            return;
        }
        task->conn = (int )((_id << 16 ) | _taos_conn_ptr);
        _taos_conn_ptr += 1;
        if (_taos_conn_ptr > UINT16_MAX) {
            _taos_conn_ptr = 0;
        }
        _taos_conn[task->conn] = conn;
        std::tuple<nifpp::str_atom, int> ok_tup = std::tie("ok", task->conn);
        this->reply(task, nifpp::make(_env, ok_tup));
    } else {
        this->reply_error(task, "conflict_conn");
    }
}

void Worker::taos_close(ErlTask *task) {
    auto it = _taos_conn.find(task->conn);
    if (it != _taos_conn.end()) {
        ::taos_close(it->second);
        this->reply(task, nifpp::make(_env, nifpp::str_atom("ok")));
    } else {
        this->reply_error(task, "invalid_conn");
    }
}

void Worker::taos_get_server_info(ErlTask *task) {
    auto it = _taos_conn.find(task->conn);
    if (it != _taos_conn.end()) {
        const char* server_info = ::taos_get_server_info(it->second);
        nifpp::str_atom atom_ok("ok");
        nifpp::binary server_bin(server_info);
        auto ok_tup = std::tie(atom_ok, server_bin);
        this->reply(task, nifpp::make(_env, ok_tup));
    } else {
        this->reply_error(task, "invalid_conn");
    }
}

void Worker::taos_get_client_info(ErlTask *task) {
    const char* client_info = ::taos_get_client_info();
    nifpp::str_atom atom_ok("ok");
    nifpp::binary client_bin(client_info);
    auto ok_tup = std::tie(atom_ok, client_bin);
    this->reply(task, nifpp::make(_env, ok_tup));
}

void Worker::taos_get_current_db(ErlTask *pTask) {
    auto it = _taos_conn.find(pTask->conn);
    if (it != _taos_conn.end()) {
        char db_name[TAOS_DB_NAME_LEN_MAX];
        int required_len = 0;
        memset(db_name, 0, TAOS_DB_NAME_LEN_MAX);
        if (::taos_get_current_db(it->second, db_name, TAOS_DB_NAME_LEN_MAX, &required_len) != 0) {
            this->reply_error(pTask, "db_name_too_big");
            return;
        }
        nifpp::str_atom atom_ok("ok");
        nifpp::binary db_name_bin(db_name);
        auto ok_tup = std::tie(atom_ok, db_name_bin);
        this->reply(pTask, nifpp::make(_env, ok_tup));
    } else {
        this->reply_error(pTask, "invalid_conn");
    }
}

void Worker::taos_select_db(ErlTask *pTask) {
    auto it = _taos_conn.find(pTask->conn);
    if (it != _taos_conn.end()) {
        auto* args = static_cast<TaskQueryArgs*>(pTask->args);
        if (args == nullptr) {
            this->reply_error(pTask, "invalid_arg");
            return;
        }
        if(::taos_select_db(it->second, args->sql.c_str()) != 0) {
            this->reply_error_str(pTask, ::taos_errstr(nullptr));
            return;
        }
        this->reply(pTask, nifpp::make(_env, nifpp::str_atom("ok")));
    } else {
        this->reply_error(pTask, "invalid_conn");
    }
}


void Worker::taos_query(ErlTask *pTask) {
    auto it = _taos_conn.find(pTask->conn);
    if (it != _taos_conn.end()) {
        auto* args = static_cast<TaskQueryArgs*>(pTask->args);
        if (args == nullptr) {
            this->reply_error(pTask, "invalid_arg");
            return;
        }
        TAOS_RES* res = ::taos_query(it->second, args->sql.c_str());
        if (taos_errno(res) != 0) {
            this->reply_error_str(pTask, ::taos_errstr(res));
            return;
        }
        this->taos_query_res(pTask, res);
    } else {
        this->reply_error(pTask, "invalid_conn");
    }
}

void Worker::taos_query_a(ErlTask *pTask) {
    this->reply_error(pTask, "not_implemented");
}

void Worker::taos_query_res(ErlTask *pTask, TAOS_RES *res) {
    int precision = ::taos_result_precision(res);
    int affected_rows = ::taos_affected_rows(res);
    int field_count = ::taos_field_count(res);
    TAOS_FIELD* fields = ::taos_fetch_fields(res);
    TAOS_ROW  row = nullptr;
    std::vector<nifpp::TERM> header, body;
    if (fields == nullptr && field_count > 0) {
        ::taos_free_result(res);
        this->reply_error_str(pTask, "taos_fetch_fields -> NULL");
        return;
    }
    // header
    for (int i = 0; i < field_count; i++) {
        std::string field_name(fields[i].name);
        int field_type = fields[i].type;
        auto tup = std::tie(field_name, field_type);
        header.push_back(nifpp::make(_env, tup));
    }
    // row
    while ((row = ::taos_fetch_row(res))) {
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
        std::vector<nifpp::TERM> row_data;
        for(int i = 0; i < field_count; i++) {
            if(fields[i].bytes == 0) {
                row_data.push_back(nifpp::make(_env, nifpp::str_atom("nil")));
                continue;
            }
            switch (fields[i].bytes) {
                case TSDB_DATA_TYPE_NULL:
                    row_data.push_back(nifpp::make(_env, nifpp::str_atom("nil")));
                    break;
                case TSDB_DATA_TYPE_BOOL:
                    int8_val = *((int8_t *)row[i]);
                    if(int8_val) {
                        row_data.push_back(nifpp::make(_env, nifpp::str_atom("true")));
                    } else {
                        row_data.push_back(nifpp::make(_env, nifpp::str_atom("false")));
                    }
                    break;
                case TSDB_DATA_TYPE_TINYINT:
                    int8_val = *((int8_t *)row[i]);
                    row_data.push_back(nifpp::make(_env, int8_val));
                    break;
                case TSDB_DATA_TYPE_UTINYINT:
                    uint8_val = *((uint8_t *)row[i]);
                    row_data.push_back(nifpp::make(_env, uint8_val));
                    break;
                case TSDB_DATA_TYPE_SMALLINT:
                    int16_val = *((int16_t *)row[i]);
                    row_data.push_back(nifpp::make(_env, int16_val));
                    break;
                case TSDB_DATA_TYPE_USMALLINT:
                    uint16_val = *((uint16_t *)row[i]);
                    row_data.push_back(nifpp::make(_env, uint16_val));
                    break;
                case TSDB_DATA_TYPE_INT:
                    int32_val = *((int32_t *)row[i]);
                    row_data.push_back(nifpp::make(_env, int32_val));
                    break;
                case TSDB_DATA_TYPE_UINT:
                    uint32_val = *((uint32_t *)row[i]);
                    row_data.push_back(nifpp::make(_env, uint32_val));
                    break;
                case TSDB_DATA_TYPE_BIGINT:
                case TSDB_DATA_TYPE_TIMESTAMP:
                    int64_val = *((int64_t *)row[i]);
                    row_data.push_back(nifpp::make(_env, int64_val));
                    break;
                case TSDB_DATA_TYPE_UBIGINT:
                    uint64_val = *((uint64_t *)row[i]);
                    row_data.push_back(nifpp::make(_env, uint64_val));
                    break;
                case TSDB_DATA_TYPE_FLOAT:
                    float_val = *((float *)row[i]);
                    row_data.push_back(nifpp::make(_env, float_val));
                    break;
                case TSDB_DATA_TYPE_DOUBLE:
                    double_val = *((double *)row[i]);
                    row_data.push_back(nifpp::make(_env, double_val));
                    break;
                default:
                    uint16_val = *((uint16_t*)((uint8_t*)row[i] - VARSTR_HEADER_SIZE));
                    nifpp::binary bin_val(uint16_val);
                    memcpy(bin_val.data, row[i], bin_val.size);
                    row_data.push_back(nifpp::make(_env, bin_val));
                    break;
            }
        }
        body.push_back(nifpp::make(_env, row_data));
    }
    // reply
    nifpp::str_atom atom_ok("ok");
    auto header_tup = std::tie(precision, affected_rows, header);
    auto msg_tup = std::tie(atom_ok, header_tup, body);
    this->reply(pTask, nifpp::make(_env, msg_tup));
}
