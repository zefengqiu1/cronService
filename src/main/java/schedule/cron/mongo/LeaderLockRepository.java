package schedule.cron.mongo;

import org.springframework.data.mongodb.repository.MongoRepository;
import schedule.cron.model.LeaderLock;

public interface LeaderLockRepository
        extends MongoRepository<LeaderLock, String> {
}
