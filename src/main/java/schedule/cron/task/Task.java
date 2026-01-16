package schedule.cron.task;

import schedule.cron.model.TaskInstance;

public interface Task {
    void execute(TaskInstance ti);
}
