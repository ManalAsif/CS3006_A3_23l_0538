#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"

#include <thread>
#include <vector>
#include <deque>
#include <memory>
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
    struct TaskGroup {
        TaskID id;
        IRunnable* runnable;
        int total_tasks;

        int next_task;
        int completed_tasks;
        int remaining_dependencies;

        bool completed;

        std::vector<TaskID> dependents;

        TaskGroup(
            TaskID task_id,
            IRunnable* task_runnable,
            int num_tasks
        )
            : id(task_id),
              runnable(task_runnable),
              total_tasks(num_tasks),
              next_task(0),
              completed_tasks(0),
              remaining_dependencies(0),
              completed(false) {
        }
    };

    int num_threads_;

    std::vector<std::thread> workers_;

    // All task groups, indexed by TaskID.
    std::vector<std::unique_ptr<TaskGroup>> groups_;

    // Groups with tasks available for execution.
    std::deque<TaskID> ready_groups_;

    std::mutex mutex_;

    std::condition_variable work_cv_;
    std::condition_variable done_cv_;

    TaskID next_group_id_;

    // Number of groups that have not completed.
    int unfinished_groups_;

    bool shutdown_;

    void workerLoop();

    // Requires mutex_ to be locked.
    void completeGroup(TaskID id);

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
