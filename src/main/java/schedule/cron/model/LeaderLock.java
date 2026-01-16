package schedule.cron.model;

import lombok.Builder;
import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Data
@Builder
@Document(collection = "leader_lock")
public class LeaderLock {

    @Id
    private String id; // scheduler_leader

    private String owner;

    private Long expireAt;
}

