package schedule.cron.queue;

import schedule.cron.model.TaskInstance;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class InMemoryTaskQueue implements TaskQueue {

    private final BlockingQueue<TaskInstance> queue = new LinkedBlockingQueue<>();

    @Override
    public void publish(TaskInstance ti) {
        queue.offer(ti);
        System.out.println("🟢 Published task: " + ti.getTaskId());
    }

    @Override
    public TaskInstance take() throws InterruptedException {
        return queue.take();
    }

    @Override
    public boolean isEmpty() {
        return queue.isEmpty();
    }
}
