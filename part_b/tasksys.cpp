#include "tasksys.h"

#include <algorithm>
#include <thread>
#include <vector>
#include <deque>
#include <memory>
#include <mutex>
#include <condition_variable>

// ============================================================
// Base Classes
// ============================================================

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}

ITaskSystem::~ITaskSystem() {}

// ============================================================
// Serial Task System
// ============================================================

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads)
    : ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(
    IRunnable* runnable,
    int num_total_tasks
) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(
    IRunnable* runnable,
    int num_total_tasks,
    const std::vector<TaskID>& deps
) {
    run(runnable, num_total_tasks);
    return 0;
}

void TaskSystemSerial::sync() {}

// ============================================================
// Parallel Task System - Always Spawn
// ============================================================

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads)
    : ITaskSystem(num_threads) {
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(
    IRunnable* runnable,
    int num_total_tasks
) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(
    IRunnable* runnable,
    int num_total_tasks,
    const std::vector<TaskID>& deps
) {
    run(runnable, num_total_tasks);
    return 0;
}

void TaskSystemParallelSpawn::sync() {}

// ============================================================
// Parallel Task System - Thread Pool Spinning
// ============================================================

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::
TaskSystemParallelThreadPoolSpinning(int num_threads)
    : ITaskSystem(num_threads) {
}

TaskSystemParallelThreadPoolSpinning::
~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(
    IRunnable* runnable,
    int num_total_tasks
) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(
    IRunnable* runnable,
    int num_total_tasks,
    const std::vector<TaskID>& deps
) {
    run(runnable, num_total_tasks);
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {}

// ============================================================
// Parallel Task System - Thread Pool Sleeping
// ============================================================

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

// ------------------------------------------------------------
// Constructor
// ------------------------------------------------------------

TaskSystemParallelThreadPoolSleeping::
TaskSystemParallelThreadPoolSleeping(int num_threads)
    : ITaskSystem(num_threads),
      num_threads_(std::max(1, num_threads)),
      next_group_id_(0),
      unfinished_groups_(0),
      shutdown_(false) {

    workers_.reserve(num_threads_);

    for (int i = 0; i < num_threads_; i++) {
        workers_.emplace_back(
            &TaskSystemParallelThreadPoolSleeping::workerLoop,
            this
        );
    }
}

// ------------------------------------------------------------
// Mark a group complete and release its dependents.
//
// This function must be called while mutex_ is locked.
// ------------------------------------------------------------

void TaskSystemParallelThreadPoolSleeping::completeGroup(
    TaskID id
) {
    // Iterative processing also handles chains of empty groups.
    std::deque<TaskID> completion_queue;
    completion_queue.push_back(id);

    while (!completion_queue.empty()) {
        TaskID current_id = completion_queue.front();
        completion_queue.pop_front();

        TaskGroup* group = groups_[current_id].get();

        if (group->completed) {
            continue;
        }

        group->completed = true;
        unfinished_groups_--;

        for (TaskID dependent_id : group->dependents) {
            TaskGroup* dependent = groups_[dependent_id].get();

            dependent->remaining_dependencies--;

            if (dependent->remaining_dependencies == 0) {
                if (dependent->total_tasks == 0) {
                    completion_queue.push_back(dependent_id);
                } else {
                    ready_groups_.push_back(dependent_id);
                }
            }
        }
    }

    // Wake workers for newly released groups.
    work_cv_.notify_all();

    // Wake any thread waiting in sync().
    done_cv_.notify_all();
}

// ------------------------------------------------------------
// Worker thread
// ------------------------------------------------------------

void TaskSystemParallelThreadPoolSleeping::workerLoop() {
    while (true) {
        TaskGroup* group;
        int task_id;
        int total_tasks;
        IRunnable* runnable;

        {
            std::unique_lock<std::mutex> lock(mutex_);

            work_cv_.wait(lock, [&]() {
                return shutdown_ || !ready_groups_.empty();
            });

            if (shutdown_) {
                return;
            }

            TaskID group_id = ready_groups_.front();
            ready_groups_.pop_front();

            group = groups_[group_id].get();

            task_id = group->next_task++;
            total_tasks = group->total_tasks;
            runnable = group->runnable;

            // Make the group available to another worker
            // if it still contains unassigned tasks.
            if (group->next_task < group->total_tasks) {
                ready_groups_.push_back(group_id);
                work_cv_.notify_one();
            }
        }

        // Execute without holding the scheduler mutex.
        runnable->runTask(task_id, total_tasks);

        {
            std::lock_guard<std::mutex> lock(mutex_);

            group->completed_tasks++;

            if (group->completed_tasks == group->total_tasks) {
                completeGroup(group->id);
            }
        }
    }
}

// ------------------------------------------------------------
// Destructor
// ------------------------------------------------------------

TaskSystemParallelThreadPoolSleeping::
~TaskSystemParallelThreadPoolSleeping() {

    // Ensure all submitted work has finished.
    sync();

    {
        std::lock_guard<std::mutex> lock(mutex_);
        shutdown_ = true;
    }

    work_cv_.notify_all();

    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
}

// ------------------------------------------------------------
// Synchronous execution
// ------------------------------------------------------------

void TaskSystemParallelThreadPoolSleeping::run(
    IRunnable* runnable,
    int num_total_tasks
) {
    TaskID id = runAsyncWithDeps(
        runnable,
        num_total_tasks,
        std::vector<TaskID>()
    );

    std::unique_lock<std::mutex> lock(mutex_);

    done_cv_.wait(lock, [&]() {
        return groups_[id]->completed;
    });
}

// ------------------------------------------------------------
// Asynchronous execution with dependencies
// ------------------------------------------------------------

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(
    IRunnable* runnable,
    int num_total_tasks,
    const std::vector<TaskID>& deps
) {
    std::lock_guard<std::mutex> lock(mutex_);

    TaskID id = next_group_id_++;

    std::unique_ptr<TaskGroup> new_group(
        new TaskGroup(id, runnable, std::max(0, num_total_tasks))
    );

    // Ignore repeated dependencies.
    std::vector<TaskID> unique_deps = deps;

    std::sort(unique_deps.begin(), unique_deps.end());

    unique_deps.erase(
        std::unique(unique_deps.begin(), unique_deps.end()),
        unique_deps.end()
    );

    for (TaskID dep_id : unique_deps) {
        // Valid dependencies reference earlier groups.
        if (dep_id < 0 || dep_id >= id) {
            continue;
        }

        TaskGroup* dependency = groups_[dep_id].get();

        // A completed dependency is already satisfied.
        if (dependency->completed) {
            continue;
        }

        new_group->remaining_dependencies++;

        dependency->dependents.push_back(id);
    }

    groups_.push_back(std::move(new_group));

    unfinished_groups_++;

    TaskGroup* group = groups_[id].get();

    // No dependencies remain.
    if (group->remaining_dependencies == 0) {
        if (group->total_tasks == 0) {
            completeGroup(id);
        } else {
            ready_groups_.push_back(id);
            work_cv_.notify_one();
        }
    }

    return id;
}

// ------------------------------------------------------------
// Wait for all previously submitted groups
// ------------------------------------------------------------

void TaskSystemParallelThreadPoolSleeping::sync() {
    std::unique_lock<std::mutex> lock(mutex_);

    // Capture the groups submitted before this call.
    TaskID target = next_group_id_;

    done_cv_.wait(lock, [&]() {
        for (TaskID id = 0; id < target; id++) {
            if (!groups_[id]->completed) {
                return false;
            }
        }

        return true;
    });
}
