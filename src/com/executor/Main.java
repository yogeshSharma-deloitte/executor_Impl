package com.executor;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;

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

    static class TaskExecutorImpl implements TaskExecutor {
        BlockingQueue<Callable> taskQueue;
        Map<UUID, Object> taskGroup;
        ExecutorService executorService;
        int maxConcurrency;

        public TaskExecutorImpl(int maxConcurrency) {
            taskQueue = new LinkedBlockingQueue<>();
            this.maxConcurrency = maxConcurrency;
            taskGroup = new ConcurrentHashMap<>();
            executorService = Executors.newFixedThreadPool(maxConcurrency);
            startTaskExecution();
        }

        private void startTaskExecution() {
            for (int i = 1; i < maxConcurrency; i++) {
                executorService.submit(() -> {
                    try {
                        Callable task = taskQueue.take();
                        task.call();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

            }
        }


        @Override
        public <T> Future<T> submitTask(Task<T> task) {
            CompletableFuture<T> completableFuture = new CompletableFuture<>();
            Object computeIfAbsent = taskGroup.computeIfAbsent(task.taskGroup().groupUUID(), k -> new Object());
            synchronized (computeIfAbsent) {
                try {
                    T future = task.taskAction().call();
                    completableFuture.complete(future);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            return completableFuture;

        }



    }

    public static void main(String[] args) {

        TaskExecutorImpl taskExecutor = new TaskExecutorImpl(10);
        TaskGroup taskGroup1 = new TaskGroup(UUID.randomUUID());
        TaskGroup taskGroup2 = new TaskGroup(UUID.randomUUID());
        TaskGroup taskGroup3 = new TaskGroup(UUID.randomUUID());
        Task task1 = new Task(UUID.randomUUID(), taskGroup1, TaskType.READ, () -> {

            return "Task 1";
        });

        Task task2 = new Task(UUID.randomUUID(), taskGroup2, TaskType.READ, () -> {

            return "Task 2";
        });
        Task task3 = new Task(UUID.randomUUID(), taskGroup3, TaskType.WRITE, () -> {

            return "Task 3";
        });

        Future future1 = taskExecutor.submitTask(task1);
        Future future2 = taskExecutor.submitTask(task2);
        Future future3 = taskExecutor.submitTask(task3);

        try {
            System.out.println(future1.get());
            System.out.println(future2.get());
            System.out.println(future3.get());

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }


    }


}
