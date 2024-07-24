package com.executor;

import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

public class Main {
    public enum TaskType {
        READ,
        WRITE,
    }

    public interface TaskExecutor {
        <T> Future<T> submitTask(Task<T> task);
    }

    public record Task<T>(
            UUID taskUUID,
            TaskGroup taskGroup,
            TaskType taskType,
            Callable<T> taskAction
    ) {
        public Task {
            if (taskUUID == null || taskGroup == null || taskType == null || taskAction == null) {
                throw new IllegalArgumentException("All parameters must not be null");
            }
        }
    }

    public record TaskGroup(
            UUID groupUUID
    ) {
        public TaskGroup {
            if (groupUUID == null) {
                throw new IllegalArgumentException("All parameters must not be null");
            }
        }
    }

    public static class TaskExecutorImpl implements TaskExecutor {
        private final ExecutorService executorService;
        private final int maxConcurrency;
        private final BlockingQueue<Runnable> taskQueue;
        private final ConcurrentHashMap<UUID, ReentrantLock> groupLocks;

        public TaskExecutorImpl(int maxConcurrency) {
            this.maxConcurrency = maxConcurrency;
            this.executorService = Executors.newFixedThreadPool(maxConcurrency);
            this.taskQueue = new LinkedBlockingQueue<>();
            this.groupLocks = new ConcurrentHashMap<>();
            startTaskExecutor();
        }

        private void startTaskExecutor() {
            executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        while (true) {
                            try {
                                Runnable task = taskQueue.take();
                                task.run();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                break;
                            }
                        }
                    }
                });
            }


        @Override
        public <T> Future<T> submitTask(Task<T> task) {
            CompletableFuture<T> future = new CompletableFuture<>();
            Runnable wrappedTask = new Runnable() {
                @Override
                public void run() {
                    ReentrantLock lock = groupLocks.computeIfAbsent(task.taskGroup().groupUUID(), k -> new ReentrantLock());
                    lock.lock();
                    try {
                        T result = task.taskAction().call();
                        future.complete(result);
                    } catch (Exception e) {
                        future.completeExceptionally(e);
                    } finally {
                        lock.unlock();
                    }
                }
            };
            taskQueue.offer(wrappedTask);
            return future;
        }
    }

    public static void main(String[] args) {
        TaskExecutor taskExecutor = new TaskExecutorImpl(10);

        TaskGroup group1 = new TaskGroup(UUID.randomUUID());
        TaskGroup group2 = new TaskGroup(UUID.randomUUID());

        Task<Integer> task1 = new Task<>(UUID.randomUUID(), group1, TaskType.READ, new Callable<Integer>() {
            @Override
            public Integer call() {
                System.out.println("Executing Task 1");
                return 1;
            }
        });

        Task<Integer> task2 = new Task<>(UUID.randomUUID(), group2, TaskType.WRITE, new Callable<Integer>() {
            @Override
            public Integer call() {
                System.out.println("Executing Task 2");
                return 2;
            }
        });

        Future<Integer> future1 = taskExecutor.submitTask(task1);
        Future<Integer> future2 = taskExecutor.submitTask(task2);

        try {
            System.out.println("Result of Task 1: " + future1.get());
            System.out.println("Result of Task 2: " + future2.get());
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }
}
