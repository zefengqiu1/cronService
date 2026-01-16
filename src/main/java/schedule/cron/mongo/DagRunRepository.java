package schedule.cron.mongo;

import org.springframework.data.mongodb.repository.MongoRepository;
import schedule.cron.model.DagRun;
import schedule.cron.model.DagRunStatus;

import java.util.List;
import java.util.Optional;

public interface DagRunRepository extends MongoRepository<DagRun, String> {

    Optional<DagRun> findByDagIdAndExecutionDate(
            String dagId,
            Long executionDate
    );

    List<DagRun> findByDagIdAndStatus(
            String dagId,
            DagRunStatus status
    );
}

