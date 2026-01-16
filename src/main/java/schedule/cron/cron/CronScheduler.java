package schedule.cron.cron;

import org.springframework.stereotype.Component;
import schedule.cron.model.*;
import schedule.cron.mongo.CronJobRepository;
import schedule.cron.mongo.DagRepository;
import schedule.cron.mongo.DagRunRepository;
import schedule.cron.mongo.TaskInstanceRepository;

import java.util.Map;
@Component
public class CronScheduler {

    private final CronJobRepository cronRepo;
    private final DagRepository dagRepo;
    private final DagRunRepository dagRunRepo;
    private final TaskInstanceRepository taskRepo;

    public CronScheduler(
            CronJobRepository cronRepo,
            DagRepository dagRepo,
            DagRunRepository dagRunRepo,
            TaskInstanceRepository taskRepo) {
        this.cronRepo = cronRepo;
        this.dagRepo = dagRepo;
        this.dagRunRepo = dagRunRepo;
        this.taskRepo = taskRepo;
    }

    public void tick() {
        long now = System.currentTimeMillis();

        // 🔹 打印当前 tick 时间
        System.out.println("🕒 CronScheduler tick at: " + now);

        for (CronJob job :
                cronRepo.findByStatusAndNextFireTimeLessThanEqual("ACTIVE", now)) {

            long exec = job.getNextFireTime();
            String dagId = job.getDagId();
            String dagRunId = dagId + "__" + exec;

            // 🔹 打印找到的 CronJob
            System.out.println("🔹 Found active CronJob: " + job.getDagId()
                    + ", nextFireTime=" + exec);

            if (dagRunRepo.existsById(dagRunId)) {
                System.out.println("⚪ DagRun already exists: " + dagRunId);
                continue;
            }

            Dag dag = dagRepo.findById(dagId).orElse(null);
            if (dag == null) {
                System.out.println("⚪ DAG not found: " + dagId);
                continue;
            }

            // 🔹 打印创建 DagRun
            System.out.println("🟢 Creating DagRun: " + dagRunId);
            dagRunRepo.save(DagRun.builder()
                    .dagRunId(dagRunId)
                    .dagId(dagId)
                    .executionDate(exec)
                    .status(DagRunStatus.RUNNING)
                    .createdAt(now)
                    .build());

            // 🔹 打印每个 TaskInstance 创建
            dag.getTasks().forEach((taskId, def) -> {
                String taskInstanceId = dagRunId + "__" + taskId;
                System.out.println("🟢 Creating TaskInstance: " + taskInstanceId);

                taskRepo.save(TaskInstance.builder()
                        .id(taskInstanceId)
                        .dagRunId(dagRunId)
                        .dagId(dagId)
                        .taskId(taskId)
                        .status(TaskInstanceStatus.NONE)
                        .maxRetries(def.getRetries())
                        .tryNumber(0)
                        .build());
            });

            // 🔹 打印更新 CronJob 下一次触发
            long nextFireTime = exec + 300_000; // 下次触发时间 5 分钟后
            System.out.println("🔹 Updating CronJob nextFireTime to: " + nextFireTime);
            cronRepo.save(job.toBuilder()
                    .nextFireTime(nextFireTime)
                    .updatedAt(now)
                    .build());

        }
    }

}
