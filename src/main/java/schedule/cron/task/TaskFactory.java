package schedule.cron.task;

import java.util.Map;

public class TaskFactory {

    private static final Map<String, Task> TASKS = Map.of(
            "print", new PrintTask()
    );

    public static Task get(String taskId) {
        return TASKS.get(taskId);
    }
}


