package schedule.cron.queue;

import schedule.cron.model.TaskInstance;

public interface TaskQueue {
    void publish(TaskInstance ti);
    TaskInstance take() throws InterruptedException;
    boolean isEmpty();
}
