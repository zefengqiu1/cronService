package schedule.cron.task;

import schedule.cron.model.TaskInstance;

public class PrintTask implements Task {
    @Override
    public void execute(TaskInstance ti) {
        System.out.println("🌟 Executing task: " + ti.getTaskId() +
                " from DAG: " + ti.getDagId() +
                " runId: " + ti.getDagRunId());
    }
}
