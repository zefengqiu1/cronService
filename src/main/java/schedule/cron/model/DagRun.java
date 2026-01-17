package schedule.cron.model;

import lombok.Builder;
import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.mapping.Document;

@Data
@Builder(toBuilder = true)
@Document(collection = "dag_run")
@CompoundIndex(
        name = "dag_execution_idx",
        def = "{'dagId': 1, 'executionDate': 1}",
        unique = true
)
@CompoundIndex(name = "status_dagrun_idx",
        def = "{'status': 1, 'dagRunId': 1}")  // 加速 upstreamDone 查询
public class DagRun {

    @Id
    private String dagRunId;

    private String dagId;

    private Long executionDate;

    private DagRunStatus status;

    private Long startTime;

    private Long endTime;

    private Long createdAt;

}
