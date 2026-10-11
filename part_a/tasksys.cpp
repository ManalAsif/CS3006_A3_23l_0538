#include "tasksys.h"

#include <thread>
#include <vector>
#include <atomic>
#include <algorithm>


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}

ITaskSystem::~ITaskSystem() {}


/*
 * ================================================================
 * Serial Task System
 * ================================================================
 */

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
    return 0;
}

void TaskSystemSerial::sync() {
    return;
}


/*
 * ================================================================
 * Parallel Task System - Always Spawn
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads)
    : ITaskSystem(num_threads),
      num_threads_(num_threads) {
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {
}

void TaskSystemParallelSpawn::run(
    IRunnable* runnable,
    int num_total_tasks
) {
    if (num_total_tasks <= 0) {
        return;
    }

    int workers = std::min(
        std::max(1, num_threads_),
        num_total_tasks
    );

    std::vector<std::thread> threads;

    for (int t = 0; t < workers; t++) {
        threads.emplace_back([=]() {
            for (int i = t; i < num_total_tasks; i += workers) {
                runnable->runTask(i, num_total_tasks);
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(
    IRunnable* runnable,
    int num_total_tasks,
    const std::vector<TaskID>& deps
) {
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    return;
}


/*
 * ================================================================
 * Parallel Thread Pool - Spinning
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}


/*
 * Constructor
 *
 * Creates persistent worker threads.
 */
TaskSystemParallelThreadPoolSpinning::
TaskSystemParallelThreadPoolSpinning(int num_threads)
    : ITaskSystem(num_threads),
      num_threads_(std::max(1, num_threads)),
      next_task_(0),
      completed_tasks_(0),
      finished_workers_(0),
      generation_(0),
      shutdown_(false),
      current_runnable_(nullptr),
      total_tasks_(0) {

    for (int i = 0; i < num_threads_; i++) {
        workers_.emplace_back(
            &TaskSystemParallelThreadPoolSpinning::workerLoop,
            this
        );
    }
}


/*
 * Worker Loop
 *
 * Workers continuously check for new task batches.
 */
void TaskSystemParallelThreadPoolSpinning::workerLoop() {

    int observed_generation = 0;

    while (!shutdown_.load()) {

        int current_generation = generation_.load();

        // No new batch available
        if (current_generation == observed_generation) {
            std::this_thread::yield();
            continue;
        }

        // New batch detected
        observed_generation = current_generation;

        IRunnable* runnable = current_runnable_;
        int total = total_tasks_;

        // Dynamically claim tasks
        while (true) {

            int task_id = next_task_.fetch_add(1);

            // No tasks remaining
            if (task_id >= total) {
                break;
            }

            // Execute task
            runnable->runTask(task_id, total);

            // Mark task as completed
            completed_tasks_.fetch_add(1);
        }

        // This worker has finished the current batch
        finished_workers_.fetch_add(1);
    }
}


/*
 * Destructor
 *
 * Signals workers to stop and joins them.
 */
TaskSystemParallelThreadPoolSpinning::
~TaskSystemParallelThreadPoolSpinning() {

    shutdown_.store(true);

    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
}


/*
 * run()
 *
 * Executes a batch of tasks synchronously.
 */
void TaskSystemParallelThreadPoolSpinning::run(
    IRunnable* runnable,
    int num_total_tasks
) {
    std::lock_guard<std::mutex> lock(run_mutex_);

    // No tasks to execute
    if (num_total_tasks <= 0) {
        return;
    }

    // Publish task batch information
    current_runnable_ = runnable;
    total_tasks_ = num_total_tasks;

    // Reset counters
    next_task_.store(0);
    completed_tasks_.store(0);
    finished_workers_.store(0);

    // Notify workers of new batch
    generation_.fetch_add(1);

    // Wait until all tasks complete
    while (completed_tasks_.load() < num_total_tasks) {
        std::this_thread::yield();
    }

    // Wait until all workers finish the batch
    while (finished_workers_.load() < num_threads_) {
        std::this_thread::yield();
    }
}


TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(
    IRunnable* runnable,
    int num_total_tasks,
    const std::vector<TaskID>& deps
) {
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    return;
}


/*
 * ================================================================
 * Parallel Thread Pool - Sleeping
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::
TaskSystemParallelThreadPoolSleeping(int num_threads)
    : ITaskSystem(num_threads) {
}

TaskSystemParallelThreadPoolSleeping::
~TaskSystemParallelThreadPoolSleeping() {
}

void TaskSystemParallelThreadPoolSleeping::run(
    IRunnable* runnable,
    int num_total_tasks
) {
    // Sequential starter implementation
    // Sleeping thread pool will be implemented next.

    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(
    IRunnable* runnable,
    int num_total_tasks,
    const std::vector<TaskID>& deps
) {
    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
    return;
}
