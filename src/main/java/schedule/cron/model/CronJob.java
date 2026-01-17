package schedule.cron.model;

import lombok.Builder;
import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

@Data
@Builder(toBuilder = true)
@Document(collection = "cron_job")
@CompoundIndex(name = "cron_schedule_idx",
        def = "{'status': 1, 'nextFireTime': 1}")
public class CronJob {

    @Id
    private String id;

    @Indexed(unique = true)
    private String dagId;

    private String cronExpr;

    private String timezone;

    @Indexed
    private Long nextFireTime;

    private String status; // ACTIVE | PAUSED

    private Long updatedAt;
}


