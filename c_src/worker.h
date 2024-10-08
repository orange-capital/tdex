//
// Created by noname on 23/09/2024.
//
#ifndef TAOS_C_SRC_WORKER_H
#define TAOS_C_SRC_WORKER_H
#include <mutex>
#include <atomic>
#include <vector>
#include <queue>
#include "msg.h"
#include "taos.h"

#define TASK_BATCH_SIZE 16
#define TAOS_DB_NAME_LEN_MAX 256
#define VARSTR_HEADER_SIZE sizeof(uint16_t)

class Worker {
private:
    int _id;
    std::atomic_bool _is_stop;
    ErlNifTid _tid;
    uint32_t _taos_conn_ptr;
    uint32_t _taos_stmt_ptr;
    std::map<int, TAOS*> _taos_conn;
    std::map<int, TAOS_STMT*> _taos_stmt;
    std::queue<ErlTask*> _queue;
    std::mutex _queue_mut;
public:
    explicit Worker(int id);
    ~Worker();
    void stop();
    int start();
    int process_batch();
    void run();
    void process(ErlTask *task);
    void send(ErlTask* task);

    void taos_connect(ErlTask *pTask);

    void taos_close(ErlTask *pTask);

    static inline void reply(ErlTask* task, nifpp::TERM& term);

    static inline void reply_error(ErlTask *task, const char *string);

    static inline void reply_error_str(ErlTask *task, const char *string);

    void taos_get_server_info(ErlTask *task);

    static void taos_get_client_info(ErlTask *task);

    void taos_get_current_db(ErlTask *task);

    void taos_select_db(ErlTask *task);

    void taos_query_a(ErlTask *task);

    void taos_query(ErlTask *task);

    static void taos_query_res(ErlTask *task, TAOS_RES *res);

    void taos_stmt_close(ErlTask *task);

    void taos_stmt_prepare(ErlTask *task);

    void taos_stmt_execute(ErlTask *task);
};

class WorkerManager {
private:
    static WorkerManager* _instance;
    static std::mutex _instance_mut;
    std::vector<Worker*> _workers;
    std::atomic_uint _route_ptr;

public:
    std::atomic_int _worker_count;

public:
    WorkerManager();
    ~WorkerManager();

    int start(int num);
    void stop();
    bool dispatch(ErlTask* task);

    static WorkerManager* instance();
    static void dispose();
};

#endif //TAOS_C_SRC_WORKER_H
