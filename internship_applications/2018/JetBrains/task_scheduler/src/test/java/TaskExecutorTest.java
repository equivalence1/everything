import org.junit.Test;
import static org.junit.Assert.*;

import ru.jetbrains.application_2018.work_distribution.TaskExecutor;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class TaskExecutorTest {

    @Test
    public void simpleExecutorTest() throws Exception {
        final int nTasks = 100;

        final TaskExecutor executor = new TaskExecutor(2, 10);
        final AtomicInteger counter = new AtomicInteger(0);
        final List<Future<Integer>> results = new ArrayList<>();

        for (int i = 0; i < nTasks; i++) {
            final int id = i;
            results.add(executor.submit(() -> {
                counter.incrementAndGet();
                return id;
            }));
        }

        executor.shutdown();

        for (int i = 0; i < nTasks; i++) {
            assertTrue(results.get(i).isDone());
            assertEquals(i, (long)results.get(i).get());
        }

        assertEquals(nTasks, counter.get());
    }

    @Test
    public void checkWorkIsDistributed() {
        final int nTasks = 100;

        final Set<Long> threadIds = new ConcurrentSkipListSet<>();
        final TaskExecutor executor = new TaskExecutor(2, 10);

        for (int i = 0; i < nTasks; i++) {
            executor.submit(() -> {
                try {
                    // pause execution so that we put tasks faster when handle them
                    Thread.sleep(100);
                } catch (Exception e) {
                    // ignore
                }
                threadIds.add(Thread.currentThread().getId());
                return null;
            });
        }

        executor.shutdown();

        assertEquals(10, threadIds.size());
    }

}
