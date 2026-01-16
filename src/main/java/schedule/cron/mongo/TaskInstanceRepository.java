package schedule.cron.mongo;

import org.springframework.data.mongodb.repository.MongoRepository;
import schedule.cron.model.TaskInstance;
import schedule.cron.model.TaskInstanceStatus;

import java.util.List;
public interface TaskInstanceRepository
        extends MongoRepository<TaskInstance, String> {

    List<TaskInstance> findTop100ByStatus(TaskInstanceStatus status);

    long countByDagRunIdAndTaskIdInAndStatusNot(
            String dagRunId,
            List<String> taskIds,
            TaskInstanceStatus status
    );
}
