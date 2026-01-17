package schedule.cron.model;

import lombok.Builder;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
@Builder
public class TaskDefinition {

    private String taskName; // ✅ 新增：对应 Worker 类型 (print, email, http, etc.)

    private List<String> upstream; // 上游任务的 taskId

    private List<String> downstream; // ✅ 新增：下游任务的 taskId (可选，用于快速查找)

    private int retries;

    private int timeoutSec;

    private int priority; // 优先级

    private Map<String, Object> params;
}