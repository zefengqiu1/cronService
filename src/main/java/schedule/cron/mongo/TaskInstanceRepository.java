// TaskInstanceRepository.java
package schedule.cron.mongo;

import org.springframework.data.mongodb.repository.MongoRepository;
import schedule.cron.model.TaskInstance;
import schedule.cron.model.TaskInstanceStatus;

import java.util.List;
import java.util.Set;

public interface TaskInstanceRepository
        extends MongoRepository<TaskInstance, String> {

    List<TaskInstance> findTop100ByStatus(TaskInstanceStatus status);

    long countByDagRunIdAndTaskNameInAndStatusNot(
            String dagRunId,
            List<String> taskNames,
            TaskInstanceStatus status
    );

    // ✅ 新增：按 dagRunId 查询所有任务
    List<TaskInstance> findByDagRunId(String dagRunId);

    // ✅ 新增：批量查询多个 dagRun 的任务（用于性能优化）
    List<TaskInstance> findByDagRunIdIn(Set<String> dagRunIds);

    // ✅ 新增：根据 dagRunId 和 taskId 列表查询
    List<TaskInstance> findByDagRunIdAndTaskNameIn(String dagRunId, List<String> taskNames);

    // ✅ 新增：查询超时的运行中任务（用于故障恢复）
    List<TaskInstance> findByStatusAndLastHeartbeatLessThan(
            TaskInstanceStatus status,
            Long timestamp
    );
}