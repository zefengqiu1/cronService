package schedule.cron.publisher;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import schedule.cron.model.TaskInstance;

@Component
public class TaskPublisher {

    private static final Logger log = LoggerFactory.getLogger(TaskPublisher.class);

    private final KafkaTemplate<String, String> kafkaTemplate; // ✅ String 类型
    private final ObjectMapper objectMapper;

    @Value("${scheduler.kafka.topic.prefix:task-}")
    private String topicPrefix;

    @Value("${scheduler.kafka.topic.dag-completion:dag-completion}")
    private String dagCompletionTopic;

    public TaskPublisher(KafkaTemplate<String, String> kafkaTemplate,
                         ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    /**
     * 发送任务到对应的 Kafka Topic
     * Topic = task-{taskName}
     */
    public void publishTask(TaskInstance ti) {
        try {
            String topic = topicPrefix + ti.getTaskName();
            String json = objectMapper.writeValueAsString(ti); // ✅ 手动序列化

            kafkaTemplate.send(topic, ti.getId(), json)
                    .whenComplete((result, ex) -> {
                        if (ex == null) {
                            log.info("🟢 Published task to {}: id={}, partition={}, offset={}",
                                    topic,
                                    ti.getId(),
                                    result.getRecordMetadata().partition(),
                                    result.getRecordMetadata().offset());
                        } else {
                            log.error("❌ Failed to publish task to " + topic + ": " + ti.getId(), ex);
                        }
                    });
        } catch (Exception e) {
            log.error("❌ Error publishing task: " + ti.getId(), e);
            throw new RuntimeException("Failed to publish task", e);
        }
    }

    /**
     * 发送 DagRun 完成事件
     */
    public void publishDagCompletion(String dagRunId) {
        try {
            kafkaTemplate.send(dagCompletionTopic, dagRunId, dagRunId)
                    .whenComplete((result, ex) -> {
                        if (ex == null) {
                            log.info("🟢 Published DagRun completion: {}", dagRunId);
                        } else {
                            log.error("❌ Failed to publish DagRun completion: " + dagRunId, ex);
                        }
                    });
        } catch (Exception e) {
            log.error("❌ Error publishing DagRun completion: " + dagRunId, e);
        }
    }
}