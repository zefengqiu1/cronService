package schedule.cron.dag;

import org.springframework.stereotype.Component;
import schedule.cron.model.Dag;
import schedule.cron.model.TaskInstance;
import schedule.cron.model.TaskInstanceStatus;
import schedule.cron.mongo.DagRepository;
import schedule.cron.mongo.TaskInstanceOps;
import schedule.cron.mongo.TaskInstanceRepository;
import schedule.cron.queue.TaskQueue;

import java.util.List;
@Component
public class DagScheduler {

    private final TaskInstanceRepository taskRepo;
    private final DagRepository dagRepo;
    private final TaskInstanceOps ops;
    private final TaskQueue queue;

    public DagScheduler(
            TaskInstanceRepository taskRepo,
            DagRepository dagRepo,
            TaskInstanceOps ops,
            TaskQueue queue) {
        this.taskRepo = taskRepo;
        this.dagRepo = dagRepo;
        this.ops = ops;
        this.queue = queue;
    }

    public void tick() {
        for (TaskInstance ti :
                taskRepo.findTop100ByStatus(TaskInstanceStatus.NONE)) {

            if (upstreamDone(ti) && ops.markScheduled(ti.getId())) {
                queue.publish(ti);
//                System.out.println("🟢 Published task: " + ti.getTaskId());
            }
        }
    }

    private boolean upstreamDone(TaskInstance ti) {
        Dag dag = dagRepo.findById(ti.getDagId()).orElse(null);
        if (dag == null) return false;

        List<String> upstream =
                dag.getTasks().get(ti.getTaskId()).getUpstream();

        return upstream == null || upstream.isEmpty()
                || taskRepo.countByDagRunIdAndTaskIdInAndStatusNot(
                ti.getDagRunId(),
                upstream,
                TaskInstanceStatus.SUCCESS
        ) == 0;
    }
}
