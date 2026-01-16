package schedule.cron.model;

import lombok.Builder;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
@Builder
public class TaskDefinition {

    private List<String> upstream;

    private int retries;

    private int timeoutSec;

    private Map<String, Object> params;
}
