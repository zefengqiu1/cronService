package schedule.cron.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import schedule.cron.model.TaskInstance;

import java.util.concurrent.CompletableFuture;

public class KafkaTaskQueue implements TaskQueue {

    private static final Logger log = LoggerFactory.getLogger(KafkaTaskQueue.class);

    private final KafkaTemplate<String, TaskInstance> kafkaTemplate;
    private final String topic;

    public KafkaTaskQueue(KafkaTemplate<String, TaskInstance> kafkaTemplate, String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    @Override
    public void publish(TaskInstance ti) {
        try {
            // 使用 taskId 作为 key，保证同一个 task 的消息顺序
            CompletableFuture<SendResult<String, TaskInstance>> future =
                    kafkaTemplate.send(topic, ti.getTaskName(), ti);

            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    log.info("🟢 Published task to Kafka: taskName={}, partition={}, offset={}",
                            ti.getTaskName(),
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset());
                } else {
                    log.error("❌ Failed to publish task to Kafka: " + ti.getTaskName(), ex);
                }
            });
        } catch (Exception e) {
            log.error("❌ Error publishing task to Kafka: " + ti.getTaskName(), e);
            throw new RuntimeException("Failed to publish task", e);
        }
    }

    @Override
    public TaskInstance take() throws InterruptedException {
        // Kafka 使用 Listener 模式，不需要主动 take
        throw new UnsupportedOperationException(
                "Kafka uses listener pattern. Use @KafkaListener instead.");
    }

    @Override
    public boolean isEmpty() {
        // Kafka 无法直接判断是否为空，返回 false
        return false;
    }
}