package com.inspection.server.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.inspection.server.entity.DeviceData;
import com.inspection.server.repository.DeviceDataRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Kafka 消费者：订阅 device-telemetry 主题，
 * 把收到的 JSON 解析成 {@link DeviceData} 写入 MongoDB。
 */
@Component
public class KafkaDataConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaDataConsumer.class);

    private final DeviceDataRepository repository;
    private final ObjectMapper objectMapper;

    @Value("${inspection.kafka.topic.device-telemetry:device-telemetry}")
    private String topic;

    public KafkaDataConsumer(DeviceDataRepository repository, ObjectMapper objectMapper) {
        this.repository = repository;
        this.objectMapper = objectMapper;
    }

    /**
     * 监听 device-telemetry 主题，逐条处理消息。
     *
     * @param payload Kafka 消息体（JSON 字符串）
     */
    @KafkaListener(
            topics = "${inspection.kafka.topic.device-telemetry:device-telemetry}",
            groupId = "${spring.kafka.consumer.group-id:inspection-server}"
    )
    public void consume(String payload) {
        log.debug("Received message from topic [{}]: {}", topic, payload);
        try {
            DeviceData data = objectMapper.readValue(payload, DeviceData.class);
            DeviceData saved = repository.save(data);
            log.info("Saved telemetry: deviceId={}, status={}, dbId={}",
                    saved.getDeviceId(), saved.getStatus(), saved.getId());
        } catch (Exception e) {
            // 单条消息失败不要拖垮整个 listener 容器
            log.error("Failed to process message: {}. Error: {}", payload, e.getMessage(), e);
        }
    }
}
