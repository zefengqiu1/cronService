package schedule.cron.leader;

import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.*;
import org.springframework.stereotype.Component;
import schedule.cron.model.LeaderLock;

import java.util.UUID;
@Component
public class LeaderElector {

    private static final String ID = "scheduler_leader";
    private static final long TTL = 10_000;

    private final MongoTemplate template;
    private final String owner = UUID.randomUUID().toString();

    public LeaderElector(MongoTemplate template) {
        this.template = template;
    }

    public boolean isLeader() {
        long now = System.currentTimeMillis();

        Query q = Query.query(
                Criteria.where("_id").is(ID)
                        .orOperator(
                                Criteria.where("expireAt").lt(now),
                                Criteria.where("owner").is(owner)
                        )
        );

        Update u = new Update()
                .set("_id", ID)
                .set("owner", owner)
                .set("expireAt", now + TTL);

        return template.upsert(q, u, LeaderLock.class)
                .getModifiedCount() > 0;
    }
}
