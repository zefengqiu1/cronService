package schedule.cron.leader;

import com.mongodb.client.result.UpdateResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;
import schedule.cron.model.LeaderLock;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.UUID;

@Component
public class LeaderElector {

    private static final Logger log = LoggerFactory.getLogger(LeaderElector.class);
    private static final String LOCK_ID = "scheduler_leader";
    private static final long TTL = 10_000; // 10秒

    private final MongoTemplate template;
    private final String owner;

    public LeaderElector(MongoTemplate template) {
        this.template = template;
        this.owner = UUID.randomUUID().toString();
    }

    @PostConstruct
    public void init() {
        // 确保集合和文档存在
        ensureLockDocument();
        log.info("LeaderElector initialized: owner={}", owner);
    }

    @PreDestroy
    public void cleanup() {
        releaseLock();
    }

    /**
     * 确保锁文档存在
     */
    private void ensureLockDocument() {
        try {
            // 使用 upsert 但不设置 _id
            Query query = Query.query(Criteria.where("_id").is(LOCK_ID));
            Update update = new Update()
                    .setOnInsert("_id", LOCK_ID)
                    .setOnInsert("owner", "initial")
                    .setOnInsert("expireAt", 0L);

            template.upsert(query, update, LeaderLock.class);
        } catch (Exception e) {
            log.debug("Lock document initialization: {}", e.getMessage());
        }
    }

    /**
     * 尝试获取或续约 Leader 锁
     */
    public boolean isLeader() {
        long now = System.currentTimeMillis();
        long newExpireAt = now + TTL;

        try {
            // 尝试获取过期的锁或续约自己的锁
            Query query = Query.query(
                    Criteria.where("_id").is(LOCK_ID)
                            .orOperator(
                                    Criteria.where("expireAt").lt(now),    // 锁已过期
                                    Criteria.where("owner").is(owner)       // 我是当前 owner
                            )
            );

            Update update = new Update()
                    .set("owner", owner)
                    .set("expireAt", newExpireAt);

            UpdateResult result = template.updateFirst(query, update, LeaderLock.class);

            boolean acquired = result.getModifiedCount() > 0;

            if (acquired) {
                log.trace("Leader lock status: LEADER (owner={})", owner);
            }

            return acquired;

        } catch (Exception e) {
            log.error("Error in leader election", e);
            return false;
        }
    }

    /**
     * 释放锁（可选，用于优雅关闭）
     */
    public void releaseLock() {
        try {
            Query query = Query.query(
                    Criteria.where("_id").is(LOCK_ID)
                            .and("owner").is(owner)
            );

            Update update = new Update().set("expireAt", 0L);

            UpdateResult result = template.updateFirst(query, update, LeaderLock.class);

            if (result.getModifiedCount() > 0) {
                log.info("Released leader lock: owner={}", owner);
            }
        } catch (Exception e) {
            log.error("Error releasing leader lock", e);
        }
    }

    public String getOwner() {
        return owner;
    }
}