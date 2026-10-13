#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"

#include <thread>
#include <vector>
#include <atomic>
#include <mutex>
#include <condition_variable>

// ============================================================
// Serial Task System
// ============================================================

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

// ============================================================
// Parallel Task System - Always Spawn
// ============================================================

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

// ============================================================
// Parallel Task System - Thread Pool Spinning
// ============================================================

class TaskSystemParallelThreadPoolSpinning : public ITaskSystem {
private:
    int num_threads_;

    std::vector<std::thread> workers_;

    std::atomic<int> next_task_;
    std::atomic<int> completed_tasks_;
    std::atomic<int> finished_workers_;
    std::atomic<int> generation_;
    std::atomic<bool> shutdown_;

    IRunnable* current_runnable_;
    int total_tasks_;

    std::mutex run_mutex_;

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

// ============================================================
// Parallel Task System - Thread Pool Sleeping
// ============================================================

class TaskSystemParallelThreadPoolSleeping : public ITaskSystem {
private:
    int num_threads_;

    std::vector<std::thread> workers_;

    // Protects batch publication and worker wake-up
    std::mutex mutex_;
    std::condition_variable work_cv_;

    // Used by run() to wait for the batch to finish
    std::condition_variable done_cv_;

    // Prevents overlapping run() calls
    std::mutex run_mutex_;

    // Task distribution and completion
    std::atomic<int> next_task_;
    std::atomic<int> completed_tasks_;

    // Counts workers that have exited the current batch
    int finished_workers_;

    IRunnable* current_runnable_;
    int total_tasks_;

    int generation_;
    bool shutdown_;

    void workerLoop();

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
