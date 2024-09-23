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

WorkerManager::WorkerManager() : size(0), _conn_ptr(0) {
    this->_workers.clear();
}

WorkerManager::~WorkerManager() {
    this->stop();
}

int WorkerManager::start(int num) {
    if (this->size > 0) {
        return 0;
    }
    for (int i = 0; i < num; i++) {
        auto* worker = new Worker(i);
        worker->start();
        this->_workers.push_back(worker);
    }
    return 1;
}

void WorkerManager::stop() {
    for(auto & _worker : this->_workers) {
        _worker->stop();
        delete _worker;
    }
    this->_workers.clear();
    this->size = 0;
}

bool WorkerManager::dispatch(ErlTask *task) {
    if (task->conn < 0) {
        if (task->func != TAOS_FUNC::CONNECT) {
            return false;
        }
        task->conn = (int)_conn_ptr;
        _conn_ptr++;
        if (_conn_ptr > INT32_MAX) {
            _conn_ptr = 0;
        }
    }
    int slot_id = task->conn % this->size;
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
    _id = id;
    _env = enif_alloc_env();
    assert(_env != nullptr);
    _taos_conn.clear();
    _taos_stmt.clear();
}

Worker::~Worker() {
    this->stop();
    enif_free_env(_env);
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
        std::this_thread::sleep_for(std::chrono::milliseconds(10000));
    }
}

void Worker::process(ErlTask* task) {
    switch (task->func) {
        case TAOS_FUNC::CONNECT:
            break;
        default:

            break;
    }
}

void Worker::send(ErlTask *task) {
    _queue_mut.lock();
    _queue.push(task);
    _queue_mut.unlock();
}


#define TASK_BATCH_SIZE 16
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
        ret++;
    }
    return ret;
}
