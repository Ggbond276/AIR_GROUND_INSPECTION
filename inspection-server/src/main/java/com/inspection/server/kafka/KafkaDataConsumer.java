package com.inspection.server.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.inspection.server.entity.DeviceAlert;
import com.inspection.server.entity.DeviceData;
import com.inspection.server.repository.DeviceAlertRepository;
import com.inspection.server.repository.DeviceDataRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.UUID;

/**
 * Kafka 消费者集合：
 *   1) device-telemetry → 写入 MongoDB
 *   2) device-alerts    → 写入 Elasticsearch
 */
@Component
public class KafkaDataConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaDataConsumer.class);

    private final DeviceDataRepository dataRepository;
    private final DeviceAlertRepository alertRepository;
    private final ObjectMapper objectMapper;

    @Value("${inspection.kafka.topic.device-telemetry:device-telemetry}")
    private String telemetryTopic;

    public KafkaDataConsumer(DeviceDataRepository dataRepository,
                             DeviceAlertRepository alertRepository,
                             ObjectMapper objectMapper) {
        this.dataRepository = dataRepository;
        this.alertRepository = alertRepository;
        this.objectMapper = objectMapper;
    }

    /**
     * 监听 device-telemetry 主题，逐条处理遥测消息。
     */
    @KafkaListener(
            topics = "${inspection.kafka.topic.device-telemetry:device-telemetry}",
            groupId = "${spring.kafka.consumer.group-id:inspection-server}"
    )
    public void consumeTelemetry(String payload) {
        log.debug("Received telemetry from topic [{}]: {}", telemetryTopic, payload);
        try {
            DeviceData data = objectMapper.readValue(payload, DeviceData.class);
            DeviceData saved = dataRepository.save(data);
            log.info("Saved telemetry: deviceId={}, status={}, dbId={}",
                    saved.getDeviceId(), saved.getStatus(), saved.getId());
        } catch (Exception e) {
            log.error("Failed to process telemetry: {}. Error: {}", payload, e.getMessage(), e);
        }
    }

    /**
     * 监听 device-alerts 主题，解析后写入 ES。
     *
     * 消息格式（兼容两种）：
     *   {"alertMessage":"...","deviceId":"...","timestamp":1700...}
     * 或
     *   {"deviceId":"...", "alertMessage":"...", "issuedAt": 1700...}
     */
    @KafkaListener(
            topics = "${inspection.kafka.topic.device-alerts:device-alerts}",
            groupId = "${spring.kafka.consumer.group-id:inspection-server}"
    )
    public void consumeAlert(String payload) {
        log.debug("Received alert: {}", payload);
        try {
            // 读成 Map 而非强类型 POJO，兼容多种字段命名
            @SuppressWarnings("unchecked")
            Map<String, Object> map = objectMapper.readValue(payload, Map.class);

            String deviceId = strOrNull(map.get("deviceId"));
            String message  = strOrNull(map.get("alertMessage"));
            Long ts        = longOrNow(map.get("timestamp"));
            if (ts == null) ts = longOrNow(map.get("issuedAt"));

            if (deviceId == null || message == null) {
                log.warn("Alert message missing required fields: {}", payload);
                return;
            }

            // ES 要求 id 非空，这里给一个稳定 id（按内容 hash，避免重复入库）
            String id = UUID.nameUUIDFromBytes(
                    (deviceId + '|' + ts + '|' + message).getBytes()).toString();

            DeviceAlert alert = new DeviceAlert(id, deviceId, message, ts);
            DeviceAlert saved = alertRepository.save(alert);
            log.info("Saved alert to ES: id={}, deviceId={}, message='{}'",
                    saved.getId(), saved.getDeviceId(), saved.getAlertMessage());
        } catch (Exception e) {
            log.error("Failed to process alert: {}. Error: {}", payload, e.getMessage(), e);
        }
    }

    private static String strOrNull(Object o) {
        if (o == null) return null;
        String s = String.valueOf(o);
        return s.isBlank() ? null : s;
    }

    private static Long longOrNow(Object o) {
        if (o == null) return null;
        if (o instanceof Number n) return n.longValue();
        try {
            return Long.parseLong(String.valueOf(o));
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
