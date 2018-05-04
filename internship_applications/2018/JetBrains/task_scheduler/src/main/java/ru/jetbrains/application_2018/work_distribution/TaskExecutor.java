package ru.jetbrains.application_2018.work_distribution;

import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

public class TaskExecutor {

    private final TaskExecutorThread[] executorThreads;
    private TaskDistributorThread workDistributorThread;

    public TaskExecutor(int threshold, int executorsNumber) {
        executorThreads = new TaskExecutorThread[executorsNumber];
        for (int i = 0; i < executorsNumber; i++) {
            executorThreads[i] = new TaskExecutorThread();
            executorThreads[i].setThreshold(threshold);
            executorThreads[i].start();
        }
        workDistributorThread = new TaskDistributorThread(executorThreads);
        workDistributorThread.start();
    }


    public TaskExecutor(int executorsNumber) {
        this(1000, executorsNumber);
    }

    public <T> Future<T> submit(Callable<T> task) {
        return workDistributorThread.submit(task);
    }

    public void shutdown() {
        workDistributorThread.stopAndJoin();
        Arrays.stream(executorThreads).forEach(TaskExecutorThread::stopAndJoin);
    }

}
