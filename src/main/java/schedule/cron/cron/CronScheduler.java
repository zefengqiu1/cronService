package schedule.cron.cron;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import schedule.cron.model.*;
import schedule.cron.mongo.*;
import schedule.cron.publisher.TaskPublisher;

import java.util.*;

@Component
public class CronScheduler {

    private static final Logger log = LoggerFactory.getLogger(CronScheduler.class);

    private final CronJobRepository cronRepo;
    private final DagRepository dagRepo;
    private final DagRunRepository dagRunRepo;
    private final TaskInstanceRepository taskRepo;
    private final TaskPublisher taskPublisher;
    private final TaskInstanceOps ops;

    public CronScheduler(
            CronJobRepository cronRepo,
            DagRepository dagRepo,
            DagRunRepository dagRunRepo,
            TaskInstanceRepository taskRepo,
            TaskPublisher taskPublisher,
            TaskInstanceOps ops) {
        this.cronRepo = cronRepo;
        this.dagRepo = dagRepo;
        this.dagRunRepo = dagRunRepo;
        this.taskRepo = taskRepo;
        this.taskPublisher = taskPublisher;
        this.ops = ops;
    }

    public void tick() {
        long now = System.currentTimeMillis();
        log.debug("🕒 CronScheduler tick at: {}", now);

        for (CronJob job : cronRepo.findByStatusAndNextFireTimeLessThanEqual("ACTIVE", now)) {
            long exec = job.getNextFireTime();
            long nextFireTime = exec + 300_000; // 5分钟后

            // ✅ 先原子更新 nextFireTime
            long updated = cronRepo.updateNextFireTime(job.getId(), exec, nextFireTime, now);
            if (updated == 0) {
                log.debug("CronJob already processed: {}", job.getId());
                continue;
            }

            try {
                createDagRunAndTriggerTasks(job, exec, now);
            } catch (Exception e) {
                // 回滚
                cronRepo.updateNextFireTime(job.getId(), nextFireTime, exec, now);
                log.error("Failed to create DagRun, rolled back: " + job.getDagId(), e);
            }
        }
    }

    private void createDagRunAndTriggerTasks(CronJob job, long exec, long now) {
        String dagId = job.getDagId();
        String dagRunId = UUID.randomUUID().toString();

        if (dagRunRepo.existsById(dagRunId)) {
            log.warn("DagRun already exists: {}", dagRunId);
            return;
        }

        Dag dag = dagRepo.findById(dagId).orElse(null);
        if (dag == null) {
            log.error("DAG not found: {}", dagId);
            return;
        }

        log.info("🟢 Creating DagRun: {}", dagRunId);

        // 创建 DagRun
        dagRunRepo.save(DagRun.builder()
                .dagRunId(dagRunId)
                .dagId(dagId)
                .executionDate(exec)
                .status(DagRunStatus.RUNNING)
                .createdAt(now)
                .build());

        // 创建所有 TaskInstance
        List<TaskInstance> allTasks = new ArrayList<>();
        List<TaskInstance> noUpstreamTasks = new ArrayList<>();

        dag.getTasks().forEach((taskId, def) -> {
            TaskInstance ti = TaskInstance.builder()
                    .id(UUID.randomUUID().toString()) // ✅ 使用 UUID
                    .dagRunId(dagRunId)
                    .dagId(dagId)
                    .taskName(taskId)
                    .taskName(def.getTaskName()) // ✅ taskName 用于路由
                    .status(TaskInstanceStatus.NONE)
                    .maxRetries(def.getRetries())
                    .tryNumber(0)
                    .build();

            allTasks.add(ti);

            // ✅ 收集无依赖的任务
            if (def.getUpstream() == null || def.getUpstream().isEmpty()) {
                noUpstreamTasks.add(ti);
            }
        });

        // 批量保存所有 TaskInstance
        taskRepo.saveAll(allTasks);
        log.info("Created {} TaskInstances for DagRun: {}", allTasks.size(), dagRunId);

        // ✅ 发送无依赖的任务到 Kafka（可以并行执行）
        for (TaskInstance ti : noUpstreamTasks) {
            taskPublisher.publishTask(ti);
            ops.markScheduled(ti.getId());
            log.info("🚀 Published initial task: {}", ti.getTaskName());
        }
    }
}