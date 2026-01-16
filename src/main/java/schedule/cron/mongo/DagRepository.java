package schedule.cron.mongo;

import org.springframework.data.mongodb.repository.MongoRepository;
import schedule.cron.model.Dag;

import java.util.List;

public interface DagRepository extends MongoRepository<Dag, String> {

    List<Dag> findByIsActiveTrue();
}

