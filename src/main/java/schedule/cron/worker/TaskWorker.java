package schedule.cron.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import schedule.cron.model.*;
import schedule.cron.mongo.DagRepository;
import schedule.cron.mongo.TaskInstanceOps;
import schedule.cron.mongo.TaskInstanceRepository;
import schedule.cron.publisher.TaskPublisher;
import schedule.cron.task.Task;
import schedule.cron.task.TaskFactory;

import java.util.*;
import java.util.stream.Collectors;

@Component
public class TaskWorker {

    private static final Logger log = LoggerFactory.getLogger(TaskWorker.class);

    private final TaskInstanceOps ops;
    private final TaskInstanceRepository taskRepo;
    private final DagRepository dagRepo;
    private final TaskPublisher taskPublisher;
    private final ObjectMapper objectMapper;

    public TaskWorker(
            TaskInstanceOps ops,
            TaskInstanceRepository taskRepo,
            DagRepository dagRepo,
            TaskPublisher taskPublisher,
            ObjectMapper objectMapper) {
        this.ops = ops;
        this.taskRepo = taskRepo;
        this.dagRepo = dagRepo;
        this.taskPublisher = taskPublisher;
        this.objectMapper = objectMapper;
    }

    /**
     * 消费 task-print 类型的任务
     */
    @KafkaListener(topics = "task-print", groupId = "worker-print", concurrency = "3")
    public void consumePrintTask(String message, Acknowledgment ack) {
        processTask(message, ack);
    }

//    /**
//     * 消费 task-email 类型的任务
//     */
//    @KafkaListener(topics = "task-email", groupId = "worker-email", concurrency = "2")
//    public void consumeEmailTask(String message, Acknowledgment ack) {
//        processTask(message, ack);
//    }
//
//    /**
//     * 消费 task-http 类型的任务
//     */
//    @KafkaListener(topics = "task-http", groupId = "worker-http", concurrency = "5")
//    public void consumeHttpTask(String message, Acknowledgment ack) {
//        processTask(message, ack);
//    }

    // ✅ 通用任务处理逻辑
    private void processTask(String message, Acknowledgment ack) {
        TaskInstance ti = null;
        try {
            ti = objectMapper.readValue(message, TaskInstance.class);
            log.info("📥 Received task: id={}, taskName={}, ti={}",
                    ti.getId(), ti.getTaskName(),ti);

            // 1. 原子更新状态 NONE -> RUNNING
            if (!ops.markRunning(ti.getId())) {
                log.warn("⚠️ Task already running or completed: {}", ti.getId());
                ack.acknowledge();
                return;
            }

            // 2. 获取任务实现
            Task task = TaskFactory.get(ti.getTaskName());
            if (task == null) {
                log.error("❌ Task implementation not found: {}", ti.getTaskName());
                markFailed(ti);
                ack.acknowledge();
                return;
            }

            // 3. 执行任务
            log.info("🚀 Executing task: {} ", ti.getTaskName());
            task.execute(ti);

            // 4. 标记成功
            ti = taskRepo.save(ti.toBuilder()
                    .status(TaskInstanceStatus.SUCCESS)
                    .endTime(System.currentTimeMillis())
                    .build());

            log.info("✅ Task completed: {}", ti.getId());

            // 5. ✅ 触发下游任务
            triggerDownstreamTasks(ti);

            // 6. ✅ 检查 DagRun 是否完成
            checkDagRunCompletion(ti.getDagRunId());

            ack.acknowledge();

        } catch (Exception e) {
            log.error("❌ Task execution failed: " + (ti != null ? ti.getId() : "unknown"), e);
            if (ti != null) {
                markFailed(ti);
            }
            ack.acknowledge();
        }
    }

    /**
     * ✅ 触发下游任务
     */
    private void triggerDownstreamTasks(TaskInstance completedTask) {
        try {
            // 1. 获取 DAG 定义
            Dag dag = dagRepo.findById(completedTask.getDagId()).orElse(null);
            if (dag == null) {
                log.error("DAG not found: {}", completedTask.getDagId());
                return;
            }

            // 2. 找到依赖这个任务的下游任务
            List<String> downstreamTaskIds = new ArrayList<>();
            dag.getTasks().forEach((taskId, def) -> {
                List<String> upstream = def.getUpstream();
                if (upstream != null && upstream.contains(completedTask.getTaskName())) {
                    downstreamTaskIds.add(taskId);
                }
            });

            if (downstreamTaskIds.isEmpty()) {
                log.debug("No downstream tasks for: {}", completedTask.getTaskName());
                return;
            }

            log.info("Found {} downstream tasks for: {}",
                    downstreamTaskIds.size(), completedTask.getTaskName());

            // 3. 查询这些下游任务的 TaskInstance
            List<TaskInstance> downstreamTasks = taskRepo.findByDagRunIdAndTaskNameIn(
                    completedTask.getDagRunId(),
                    downstreamTaskIds
            );

            // 4. 检查每个下游任务的依赖是否都完成
            for (TaskInstance downstreamTask : downstreamTasks) {
                if (downstreamTask.getStatus() != TaskInstanceStatus.NONE) {
                    continue; // 已经处理过了
                }

                TaskDefinition taskDef = dag.getTasks().get(downstreamTask.getTaskName());

                // 检查所有 upstream 是否都成功
                if (allUpstreamCompleted(completedTask.getDagRunId(), taskDef.getUpstream())) {
                    log.info("🟢 All upstream completed, publishing downstream task: {}",
                            downstreamTask.getTaskName());
                    taskPublisher.publishTask(downstreamTask);
                } else {
                    log.debug("⏳ Waiting for upstream tasks: {}", downstreamTask.getTaskName());
                }
            }

        } catch (Exception e) {
            log.error("Error triggering downstream tasks", e);
        }
    }

    /**
     * 检查所有 upstream 任务是否都成功
     */
    private boolean allUpstreamCompleted(String dagRunId, List<String> upstreamTaskIds) {
        if (upstreamTaskIds == null || upstreamTaskIds.isEmpty()) {
            return true;
        }

        List<TaskInstance> upstreamTasks = taskRepo.findByDagRunIdAndTaskNameIn(
                dagRunId,
                upstreamTaskIds
        );

        return upstreamTasks.stream()
                .allMatch(t -> t.getStatus() == TaskInstanceStatus.SUCCESS);
    }

    /**
     * ✅ 检查 DagRun 是否所有任务都完成
     */
    private void checkDagRunCompletion(String dagRunId) {
        try {
            List<TaskInstance> allTasks = taskRepo.findByDagRunId(dagRunId);

            boolean allCompleted = allTasks.stream()
                    .allMatch(t -> t.getStatus() == TaskInstanceStatus.SUCCESS
                            || t.getStatus() == TaskInstanceStatus.FAILED);

            if (allCompleted) {
                log.info("🎉 All tasks completed for DagRun: {}", dagRunId);
                taskPublisher.publishDagCompletion(dagRunId);
            }

        } catch (Exception e) {
            log.error("Error checking DagRun completion", e);
        }
    }

    private void markFailed(TaskInstance ti) {
        try {
            taskRepo.save(ti.toBuilder()
                    .status(TaskInstanceStatus.FAILED)
                    .endTime(System.currentTimeMillis())
                    .build());
        } catch (Exception e) {
            log.error("Failed to mark task as FAILED: " + ti.getId(), e);
        }
    }
}