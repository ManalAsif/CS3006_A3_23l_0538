#include "tasksys.h"

#include <thread>
#include <vector>
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


// Constructor
TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads)
    : ITaskSystem(num_threads),
      num_threads_(num_threads) {
}


// Destructor
TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {
}


// Parallel run implementation
void TaskSystemParallelSpawn::run(
    IRunnable* runnable,
    int num_total_tasks
) {

    // No tasks to execute
    if (num_total_tasks <= 0) {
        return;
    }

    // Determine number of worker threads
    int workers = std::min(
        std::max(1, num_threads_),
        num_total_tasks
    );

    // Store worker threads
    std::vector<std::thread> threads;

    // Create worker threads
    for (int t = 0; t < workers; t++) {

        threads.emplace_back([=]() {

            // Each thread executes its assigned tasks
            for (
                int i = t;
                i < num_total_tasks;
                i += workers
            ) {

                runnable->runTask(
                    i,
                    num_total_tasks
                );
            }

        });
    }

    // Wait for every worker to finish
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


TaskSystemParallelThreadPoolSpinning::
TaskSystemParallelThreadPoolSpinning(int num_threads)
    : ITaskSystem(num_threads) {
}


TaskSystemParallelThreadPoolSpinning::
~TaskSystemParallelThreadPoolSpinning() {
}


void TaskSystemParallelThreadPoolSpinning::run(
    IRunnable* runnable,
    int num_total_tasks
) {

    // Starter implementation: sequential execution
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
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

    // Starter implementation: sequential execution
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
