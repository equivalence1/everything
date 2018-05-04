package ru.jetbrains.application_2018.work_distribution;

import java.util.concurrent.*;

/**
 * Simple implementation of worker thread.
 * Simply executes tasks in its runq
 */
public class TaskExecutorThread {

    private final Thread worker;
    private final BlockingQueue<RunnableFuture<?>> runq = new LinkedBlockingQueue<>();
    private Boolean isRunning = false;

    private int threshold = -1;
    private Runnable freeSpaceCallback;
    private Runnable noSpaceCallback;

    public TaskExecutorThread() {
        worker = new Thread(this::run);
    }

    /**
     * Submit task to runq
     * @param task task to execute
     * @param <T> type which this task returns
     * @return Future for this task
     */
    public <T> Future<T> submit(RunnableFuture<T> task) {
        runq.add(task);
        checkNoSpaceCallback();
        return task;
    }

    /**
     * Submit task to runq
     * @param task task to execute
     * @param <T> type which this task returns
     * @return Future for this task
     */
    public <T> Future<T> submit(Callable<T> task) {
        return submit(new FutureTask<>(task));
    }

    public int getRunqSize() {
        return runq.size();
    }

    /**
     * Set threshold for runq size.
     * see {@link #setFreeSpaceCallback} and {@link #setNoSpaceCallback(Runnable)}
     * It's impossible to change threshold when thread is already running
     * @param threshold new threshold
     */
    public void setThreshold(int threshold) {
        if (!isRunning) {
            this.threshold = threshold;
        } else {
            throw new IllegalStateException("Impossible to change threshold when thread is running");
        }
    }

    /**
     * Set callback to be called when runq size becomes less than threshold
     * @param freeSpaceCallback callback to call
     */
    public void setFreeSpaceCallback(Runnable freeSpaceCallback) {
        // no need in any locks here. Reference assignment is atomic and we don't
        // have any troubles with memory reordering here
        this.freeSpaceCallback = freeSpaceCallback;
    }

    /**
     * Set callback to be called when runq size becomes equal to threshold
     * @param noSpaceCallback callback to call
     */
    public void setNoSpaceCallback(Runnable noSpaceCallback) {
        // no need in any locks here. Reference assignment is atomic and we don't
        // have any troubles with memory reordering here
        this.noSpaceCallback = noSpaceCallback;
    }

    /**
     * start this thread
     */
    public void start() {
        isRunning = true;
        worker.start();
    }

    /**
     * stop this thread
     */
    public void stop() {
        isRunning = false;
        // worker might be blocked waiting for runq to be non-empty
        // send dumb task just in case
        submit(() -> null);
    }

    /**
     * waits for all tasks to complete and then stops this thread
     */
    public void stopAndJoin() {
        submit(() -> {
            stop();
            return null;
        });
        try {
            worker.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void run() {
        while (isRunning) {
            try {
                // TODO I'm not sure if it's ok to pass `-1` as timeout, so just pass a huge number
                final RunnableFuture<?> task = runq.poll((long)1e9, TimeUnit.SECONDS);
                checkFreeSpaceCallback();
                if (task != null) {
                    handleTask(task);
                }
            } catch (InterruptedException e) {
                // just ignore and continue running
            }
        }
    }

    private void checkFreeSpaceCallback() {
        if (freeSpaceCallback != null && getRunqSize() == threshold - 1) {
            freeSpaceCallback.run();
        }
    }

    private void checkNoSpaceCallback() {
        if (noSpaceCallback != null && getRunqSize() == threshold) {
            noSpaceCallback.run();
        }
    }

    /**
     * Main task handling routine. Override it if you need different behaviour
     * @param task task to handle
     */
    protected void handleTask(RunnableFuture<?> task) {
        task.run();
    }

}
