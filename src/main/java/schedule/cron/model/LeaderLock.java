package schedule.cron.model;

import lombok.Builder;
import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

@Data
@Builder
@Document(collection = "leader_lock")
public class LeaderLock {

    @Id
    private String id; // scheduler_leader

    @Indexed
    private String owner;

    @Indexed
    private Long expireAt;
}