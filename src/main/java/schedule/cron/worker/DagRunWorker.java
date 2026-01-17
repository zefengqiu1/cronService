package schedule.cron.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import schedule.cron.model.DagRun;
import schedule.cron.model.DagRunStatus;
import schedule.cron.model.TaskInstance;
import schedule.cron.model.TaskInstanceStatus;
import schedule.cron.mongo.DagRunRepository;
import schedule.cron.mongo.TaskInstanceRepository;

import java.util.List;

@Component
public class DagRunWorker {

    private static final Logger log = LoggerFactory.getLogger(DagRunWorker.class);

    private final DagRunRepository dagRunRepo;
    private final TaskInstanceRepository taskRepo;

    public DagRunWorker(DagRunRepository dagRunRepo, TaskInstanceRepository taskRepo) {
        this.dagRunRepo = dagRunRepo;
        this.taskRepo = taskRepo;
    }

    /**
     * 消费 DagRun 完成事件，更新状态
     */
    @KafkaListener(
            topics = "${scheduler.kafka.topic.dag-completion:dag-completion}",
            groupId = "dagrun-worker",
            concurrency = "2"
    )
    public void handleDagRunCompletion(String dagRunId, Acknowledgment ack) {
        try {
            log.info("📥 Received DagRun completion event: {}", dagRunId);

            DagRun dagRun = dagRunRepo.findById(dagRunId).orElse(null);
            if (dagRun == null) {
                log.error("DagRun not found: {}", dagRunId);
                ack.acknowledge();
                return;
            }

            // 查询所有任务
            List<TaskInstance> tasks = taskRepo.findByDagRunId(dagRunId);

            boolean allSuccess = tasks.stream()
                    .allMatch(t -> t.getStatus() == TaskInstanceStatus.SUCCESS);
            boolean anyFailed = tasks.stream()
                    .anyMatch(t -> t.getStatus() == TaskInstanceStatus.FAILED);

            long now = System.currentTimeMillis();

            if (allSuccess) {
                dagRunRepo.save(dagRun.toBuilder()
                        .status(DagRunStatus.SUCCESS)
                        .endTime(now)
                        .build());
                log.info("✅ DagRun completed successfully: {}", dagRunId);
            } else if (anyFailed) {
                dagRunRepo.save(dagRun.toBuilder()
                        .status(DagRunStatus.FAILED)
                        .endTime(now)
                        .build());
                log.warn("❌ DagRun failed: {}", dagRunId);
            } else {
                log.warn("⚠️ DagRun has incomplete tasks: {}", dagRunId);
            }

            ack.acknowledge();

        } catch (Exception e) {
            log.error("Error processing DagRun completion: " + dagRunId, e);
            ack.acknowledge();
        }
    }
}