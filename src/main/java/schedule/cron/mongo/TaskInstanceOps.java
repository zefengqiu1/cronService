package schedule.cron.mongo;

import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;
import schedule.cron.model.TaskInstance;
import schedule.cron.model.TaskInstanceStatus;

@Component
public class TaskInstanceOps {

    private final MongoTemplate template;

    public TaskInstanceOps(MongoTemplate template) {
        this.template = template;
    }

    public boolean markScheduled(String id) {
        Query q = Query.query(
                Criteria.where("_id").is(id)
                        .and("status").is(TaskInstanceStatus.NONE)
        );

        Update u = new Update()
                .set("status", TaskInstanceStatus.SCHEDULED);

        return template.updateFirst(q, u, TaskInstance.class)
                .getModifiedCount() == 1;
    }

    public boolean markRunning(String id) {
        long now = System.currentTimeMillis();

        Query q = Query.query(
                Criteria.where("_id").is(id)
                        .and("status").is(TaskInstanceStatus.SCHEDULED)
        );

        Update u = new Update()
                .set("status", TaskInstanceStatus.RUNNING)
                .set("startTime", now)
                .set("lastHeartbeat", now);

        return template.updateFirst(q, u, TaskInstance.class)
                .getModifiedCount() == 1;
    }
}
