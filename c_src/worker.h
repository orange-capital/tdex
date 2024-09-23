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

class Worker {
private:
    int _id;
    std::atomic_bool _is_stop;
    ErlNifTid _tid;
    ErlNifEnv* _env;
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
};

class WorkerManager {
private:
    static WorkerManager* _instance;
    static std::mutex _instance_mut;
    std::vector<Worker*> _workers;
    std::atomic_uint _conn_ptr;

public:
    std::atomic_int size;

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
