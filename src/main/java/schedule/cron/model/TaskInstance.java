package schedule.cron.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.io.Serializable;

@Data
@Builder(toBuilder = true)
@NoArgsConstructor // kafaka 反序列
@AllArgsConstructor //builder
@Document(collection = "task_instance")
@CompoundIndex(
        name = "dag_task_idx",
        def = "{'dagRunId': 1, 'taskId': 1}",
        unique = true
)
public class TaskInstance implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    private String id; // ✅ UUID

    @Indexed
    private String dagRunId;

    private String dagId;

    @Indexed
    private String taskName; // ✅ 新增：用于路由到不同的 Kafka Topic (task_a, task_b, etc.)

    @Indexed
    private TaskInstanceStatus status;

    private int tryNumber;

    private int maxRetries;

    private Long startTime;

    private Long endTime;

    @Indexed
    private Long lastHeartbeat;
}