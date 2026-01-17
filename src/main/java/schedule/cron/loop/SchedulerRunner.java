package schedule.cron.loop;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import schedule.cron.cron.CronScheduler;
import schedule.cron.leader.LeaderElector;
import schedule.cron.model.TaskInstance;
import schedule.cron.model.TaskInstanceStatus;
import schedule.cron.mongo.TaskInstanceRepository;

import java.util.ArrayList;
import java.util.List;

@Component
public class SchedulerRunner {

    private static final Logger log = LoggerFactory.getLogger(SchedulerRunner.class);

    private final CronScheduler cronScheduler;
    private final TaskInstanceRepository taskRepo;
    private final LeaderElector leaderElector;

    public SchedulerRunner(
            CronScheduler cronScheduler,
            TaskInstanceRepository taskRepo,
            LeaderElector leaderElector) {
        this.cronScheduler = cronScheduler;
        this.taskRepo = taskRepo;
        this.leaderElector = leaderElector;
    }

    // ✅ 只需要 CronScheduler，不需要 DagScheduler
    @Scheduled(fixedDelay = 5000000)
    public void tick() {
        if (!leaderElector.isLeader()) {
            return;
        }

        cronScheduler.tick();
    }

    // ✅ 故障恢复：检查超时任务（可选）
    @Scheduled(fixedDelay = 30000)
    public void recoverStaleTasks() {
        if (!leaderElector.isLeader()) {
            return;
        }

        long timeout = System.currentTimeMillis() - 300_000; // 5分钟无心跳

        List<TaskInstance> staleTasks = taskRepo.findByStatusAndLastHeartbeatLessThan(
                TaskInstanceStatus.RUNNING, timeout
        );

        if (staleTasks.isEmpty()) return;

        log.warn("Found {} stale tasks", staleTasks.size());

        List<TaskInstance> toUpdate = new ArrayList<>();
        long now = System.currentTimeMillis();

        for (TaskInstance ti : staleTasks) {
            log.error("Task {} timed out, marking as FAILED", ti.getId());
            toUpdate.add(ti.toBuilder()
                    .status(TaskInstanceStatus.FAILED)
                    .endTime(now)
                    .build());
        }
        if (!toUpdate.isEmpty()) {
            taskRepo.saveAll(toUpdate);
        }
    }
}