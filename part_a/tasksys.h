#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"

/*
 * Serial Task System
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
 * Parallel Task System - Always Spawn
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
 * Parallel Task System - Thread Pool Spinning
 */
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


/*
 * Parallel Task System - Thread Pool Sleeping
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
