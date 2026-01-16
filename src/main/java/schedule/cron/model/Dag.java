package schedule.cron.model;

import lombok.Builder;
import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Map;

@Data
@Builder
@Document(collection = "dag")
public class Dag {

    @Id
    private String dagId;

    private boolean isActive;

    private Schedule schedule;

    private Map<String, TaskDefinition> tasks;

    private Long createdAt;

    @Data
    @Builder
    public static class Schedule {
        private String type;      // cron | manual
        private String expr;
        private String timezone;
    }
}

