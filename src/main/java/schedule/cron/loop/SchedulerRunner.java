package schedule.cron.loop;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import schedule.cron.cron.CronScheduler;
import schedule.cron.dag.DagScheduler;

@Component
public class SchedulerRunner {

    private final CronScheduler cronScheduler;
    private final DagScheduler dagScheduler;

    public SchedulerRunner(CronScheduler cronScheduler, DagScheduler dagScheduler) {
        this.cronScheduler = cronScheduler;
        this.dagScheduler = dagScheduler;
    }

    @Scheduled(fixedDelay = 500000)
    public void tick() {
        cronScheduler.tick();  // 扫描 CronJob -> 生成 DagRun + TaskInstance
        dagScheduler.tick();   // 扫描 TaskInstance -> 放入队列
    }
}

