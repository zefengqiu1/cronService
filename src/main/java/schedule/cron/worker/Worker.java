package schedule.cron.worker;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import schedule.cron.model.DagRun;
import schedule.cron.model.DagRunStatus;
import schedule.cron.model.TaskInstance;
import schedule.cron.model.TaskInstanceStatus;
import schedule.cron.mongo.DagRunRepository;
import schedule.cron.mongo.TaskInstanceOps;
import schedule.cron.mongo.TaskInstanceRepository;
import schedule.cron.queue.TaskQueue;
import schedule.cron.task.Task;
import schedule.cron.task.TaskFactory;

@Component
public class Worker {

    private final TaskQueue queue;
    private final TaskInstanceOps ops;
    private final TaskInstanceRepository repo;
    private final DagRunRepository dagRunRepositorypo;
    public Worker(TaskQueue queue, TaskInstanceOps ops,
                  TaskInstanceRepository repo,
                  DagRunRepository dagRunRepositorypo) {
        this.queue = queue;
        this.ops = ops;
        this.repo = repo;
        this.dagRunRepositorypo = dagRunRepositorypo;
    }

    @Scheduled(fixedDelay = 500)
    public void work() throws InterruptedException {
        TaskInstance ti = queue.take();
        if (ti == null) return;

        if (!ops.markRunning(ti.getId())) return;

        Task task = TaskFactory.get(ti.getTaskId());
        if (task == null) return;

        try {
            task.execute(ti);

            repo.save(ti.toBuilder()
                    .status(TaskInstanceStatus.SUCCESS)
                    .endTime(System.currentTimeMillis())
                    .build());

            DagRun dagRun = dagRunRepositorypo.findById(ti.getDagId()).orElse(null);
            if (dagRun == null) return;
            dagRunRepositorypo.save(dagRun.toBuilder()
                            .status(DagRunStatus.SUCCESS)
                    .build());

        } catch (Exception e) {
            repo.save(ti.toBuilder()
                    .status(TaskInstanceStatus.FAILED)
                    .endTime(System.currentTimeMillis())
                    .build());
        }
    }
}
