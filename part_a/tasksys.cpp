#include "tasksys.h"

#include <thread>
#include <vector>
#include <atomic>
#include <algorithm>
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
    : ITaskSystem(num_threads),
      num_threads_(std::max(1, num_threads)) {
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(
    IRunnable* runnable,
    int num_total_tasks
) {
    if (num_total_tasks <= 0) {
        return;
    }

    int workers = std::min(num_threads_, num_total_tasks);

    std::atomic<int> next_task(0);
    std::vector<std::thread> threads;

    threads.reserve(workers);

    for (int t = 0; t < workers; t++) {
        threads.emplace_back([&]() {
            while (true) {
                int task_id = next_task.fetch_add(1);

                if (task_id >= num_total_tasks) {
                    break;
                }

                runnable->runTask(task_id, num_total_tasks);
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

void TaskSystemParallelSpawn::sync() {}

// ============================================================
// Parallel Task System - Thread Pool Spinning
// ============================================================

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

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

void TaskSystemParallelThreadPoolSpinning::workerLoop() {
    int observed_generation = 0;

    while (!shutdown_.load()) {
        int current_generation = generation_.load();

        if (current_generation == observed_generation) {
            std::this_thread::yield();
            continue;
        }

        observed_generation = current_generation;

        IRunnable* runnable = current_runnable_;
        int total = total_tasks_;

        while (true) {
            int task_id = next_task_.fetch_add(1);

            if (task_id >= total) {
                break;
            }

            runnable->runTask(task_id, total);

            completed_tasks_.fetch_add(1);
        }

        finished_workers_.fetch_add(1);
    }
}

TaskSystemParallelThreadPoolSpinning::
~TaskSystemParallelThreadPoolSpinning() {
    shutdown_.store(true);

    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
}

void TaskSystemParallelThreadPoolSpinning::run(
    IRunnable* runnable,
    int num_total_tasks
) {
    std::lock_guard<std::mutex> lock(run_mutex_);

    if (num_total_tasks <= 0) {
        return;
    }

    current_runnable_ = runnable;
    total_tasks_ = num_total_tasks;

    next_task_.store(0);
    completed_tasks_.store(0);
    finished_workers_.store(0);

    generation_.fetch_add(1);

    while (completed_tasks_.load() < num_total_tasks) {
        std::this_thread::yield();
    }

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

void TaskSystemParallelThreadPoolSpinning::sync() {}

// ============================================================
// Parallel Task System - Thread Pool Sleeping
// ============================================================

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

// Constructor: create persistent workers
TaskSystemParallelThreadPoolSleeping::
TaskSystemParallelThreadPoolSleeping(int num_threads)
    : ITaskSystem(num_threads),
      num_threads_(std::max(1, num_threads)),
      next_task_(0),
      completed_tasks_(0),
      finished_workers_(0),
      current_runnable_(nullptr),
      total_tasks_(0),
      generation_(0),
      shutdown_(false) {

    for (int i = 0; i < num_threads_; i++) {
        workers_.emplace_back(
            &TaskSystemParallelThreadPoolSleeping::workerLoop,
            this
        );
    }
}

// Workers sleep until a new batch is published.
void TaskSystemParallelThreadPoolSleeping::workerLoop() {
    int observed_generation = 0;

    while (true) {
        IRunnable* runnable;
        int total;

        {
            std::unique_lock<std::mutex> lock(mutex_);

            work_cv_.wait(lock, [&]() {
                return shutdown_ ||
                       generation_ != observed_generation;
            });

            if (shutdown_) {
                return;
            }

            observed_generation = generation_;

            runnable = current_runnable_;
            total = total_tasks_;
        }

        // Claim tasks atomically, without holding the mutex.
        while (true) {
            int task_id = next_task_.fetch_add(1);

            if (task_id >= total) {
                break;
            }

            runnable->runTask(task_id, total);

            completed_tasks_.fetch_add(1);
        }

        // Every worker acknowledges finishing this batch.
        // run() will not publish another batch until all
        // workers have acknowledged completion.
        {
            std::lock_guard<std::mutex> lock(mutex_);

            finished_workers_++;

            if (finished_workers_ == num_threads_) {
                done_cv_.notify_one();
            }
        }
    }
}

// Destructor: wake and terminate all workers.
TaskSystemParallelThreadPoolSleeping::
~TaskSystemParallelThreadPoolSleeping() {
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

// Execute a synchronous batch.
void TaskSystemParallelThreadPoolSleeping::run(
    IRunnable* runnable,
    int num_total_tasks
) {
    std::lock_guard<std::mutex> run_lock(run_mutex_);

    if (num_total_tasks <= 0) {
        return;
    }

    {
        std::lock_guard<std::mutex> lock(mutex_);

        current_runnable_ = runnable;
        total_tasks_ = num_total_tasks;

        next_task_.store(0);
        completed_tasks_.store(0);

        finished_workers_ = 0;

        // Publish a new batch.
        generation_++;
    }

    // Wake workers for the new batch.
    work_cv_.notify_all();

    // Wait until every worker has exited the batch.
    {
        std::unique_lock<std::mutex> lock(mutex_);

        done_cv_.wait(lock, [&]() {
            return finished_workers_ == num_threads_;
        });
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(
    IRunnable* runnable,
    int num_total_tasks,
    const std::vector<TaskID>& deps
) {
    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {}
