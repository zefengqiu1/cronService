package schedule.cron.model;

import lombok.Builder;
import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

@Data
@Builder(toBuilder = true)
@Document(collection = "task_instance")
@CompoundIndex(
        name = "dag_task_idx",
        def = "{'dagRunId': 1, 'taskId': 1}",
        unique = true
)
public class TaskInstance {

    @Id
    private String id;

    @Indexed
    private String dagRunId;

    private String dagId;

    private String taskId;

    @Indexed
    private TaskInstanceStatus status;

    private int tryNumber;

    private int maxRetries;

    private Long startTime;

    private Long endTime;

    @Indexed
    private Long lastHeartbeat;
}

