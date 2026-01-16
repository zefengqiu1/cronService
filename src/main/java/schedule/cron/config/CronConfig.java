package schedule.cron.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import schedule.cron.queue.InMemoryTaskQueue;
import schedule.cron.queue.TaskQueue;

@Configuration
public class CronConfig {

    @Bean
    TaskQueue taskQueue() {
        return new InMemoryTaskQueue();
    }
}

