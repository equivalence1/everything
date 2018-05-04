package ru.jetbrains.application_2018.work_distribution;

import java.util.Arrays;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.Semaphore;

/**
 * Simple implementation of work distributor.
 * Submits a task to the least busy worker thread.
 */
public class TaskDistributorThread extends TaskExecutorThread {

    private final TaskExecutorThread[] threads;
    private final Semaphore sema;

    public TaskDistributorThread(final TaskExecutorThread[] threads) {
        this.threads = threads;
        sema = new Semaphore(threads.length);
        setupThreads();
    }

    @Override
    protected void handleTask(RunnableFuture<?> task) {
        try {
            // if we can acquire this sema, then at least one thread has some free space in its runq
            sema.acquire();
        } catch (InterruptedException e) {
            // TODO ignore for now, not quire sure what's best to do here.
            return;
        }
        int id = getBestId();
        threads[id].submit(task);
        sema.release();
    }

    private void setupThreads() {
        Arrays.stream(threads).forEach(th -> {
            th.setFreeSpaceCallback(this::workerThreadHasSpace);
            th.setNoSpaceCallback(this::workerThreadNoSpace);
        });
    }

    private void workerThreadHasSpace() {
        sema.release();
    }

    private void workerThreadNoSpace() {
        try {
            sema.acquire();
        } catch (InterruptedException e) {
            // TODO ignore for now, not quire sure what's best to do here.
        }
    }

    private int getBestId() {
        int res = 0;
        int min = Integer.MAX_VALUE;

        for (int i = 0; i < threads.length; i++) {
            int runqSize = threads[i].getRunqSize();
            if (runqSize < min) {
                min = runqSize;
                res = i;
            }
        }

        return res;
    }

}
