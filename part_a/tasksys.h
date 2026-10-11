#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"

#include <thread>
#include <vector>
#include <atomic>
#include <mutex>

/*
 * ================================================================
 * Serial Task System
 * ================================================================
 */

class TaskSystemSerial : public ITaskSystem {
public:
    TaskSystemSerial(int num_threads);
    ~TaskSystemSerial();

    const char* name();
    void run(IRunnable* runnable, int num_total_tasks);

    TaskID runAsyncWithDeps(
        IRunnable* runnable,
        int num_total_tasks,
        const std::vector<TaskID>& deps
    );

    void sync();
};


/*
 * ================================================================
 * Parallel Task System - Always Spawn
 * ================================================================
 */

class TaskSystemParallelSpawn : public ITaskSystem {
private:
    int num_threads_;

public:
    TaskSystemParallelSpawn(int num_threads);
    ~TaskSystemParallelSpawn();

    const char* name();
    void run(IRunnable* runnable, int num_total_tasks);

    TaskID runAsyncWithDeps(
        IRunnable* runnable,
        int num_total_tasks,
        const std::vector<TaskID>& deps
    );

    void sync();
};


/*
 * ================================================================
 * Parallel Task System - Thread Pool Spinning
 * ================================================================
 */

class TaskSystemParallelThreadPoolSpinning : public ITaskSystem {
private:
    int num_threads_;

    // Persistent worker threads
    std::vector<std::thread> workers_;

    // Atomic task counters
    std::atomic<int> next_task_;
    std::atomic<int> completed_tasks_;

    // Tracks workers that have finished the current batch
    std::atomic<int> finished_workers_;

    // Identifies each new batch of tasks
    std::atomic<int> generation_;

    // Signals worker threads to terminate
    std::atomic<bool> shutdown_;

    // Current batch information
    IRunnable* current_runnable_;
    int total_tasks_;

    // Prevents concurrent calls to run()
    std::mutex run_mutex_;

    // Worker thread function
    void workerLoop();

public:
    TaskSystemParallelThreadPoolSpinning(int num_threads);
    ~TaskSystemParallelThreadPoolSpinning();

    const char* name();
    void run(IRunnable* runnable, int num_total_tasks);

    TaskID runAsyncWithDeps(
        IRunnable* runnable,
        int num_total_tasks,
        const std::vector<TaskID>& deps
    );

    void sync();
};


/*
 * ================================================================
 * Parallel Task System - Thread Pool Sleeping
 * ================================================================
 */

class TaskSystemParallelThreadPoolSleeping : public ITaskSystem {
public:
    TaskSystemParallelThreadPoolSleeping(int num_threads);
    ~TaskSystemParallelThreadPoolSleeping();

    const char* name();
    void run(IRunnable* runnable, int num_total_tasks);

    TaskID runAsyncWithDeps(
        IRunnable* runnable,
        int num_total_tasks,
        const std::vector<TaskID>& deps
    );

    void sync();
};

#endif
