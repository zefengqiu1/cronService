package schedule.cron.mongo;

import org.springframework.data.mongodb.repository.MongoRepository;
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
}

