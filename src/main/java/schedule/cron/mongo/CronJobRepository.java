package schedule.cron.mongo;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.data.mongodb.repository.Update;
import schedule.cron.model.CronJob;

import java.util.List;

public interface CronJobRepository extends MongoRepository<CronJob, String> {

    /**
     * 查找到期的 Cron Job
     */
    List<CronJob> findByStatusAndNextFireTimeLessThanEqual(
            String status,
            Long now
    );

    @Query("{ '_id': ?0, 'nextFireTime': ?1 }")
    @Update("{ '$set': { 'nextFireTime': ?2, 'updatedAt': ?3 } }")
    long updateNextFireTime(String id, long currentTime, long newTime, long updatedAt);
}

