package com.inspection.server.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.inspection.server.entity.DeviceAlert;
import com.inspection.server.entity.DeviceData;
import com.inspection.server.repository.DeviceAlertRepository;
import com.inspection.server.repository.DeviceDataRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * GM Console / 设备状态查询 + 任务下发。
 */
@RestController
@RequestMapping("/api")
public class DeviceController {

    private static final Logger log = LoggerFactory.getLogger(DeviceController.class);

    private final DeviceDataRepository repository;
    private final DeviceAlertRepository alertRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    /** 任务下发 topic，可在 application.yml 覆盖 */
    @Value("${inspection.kafka.topic.device-tasks:device-tasks}")
    private String deviceTasksTopic;

    public DeviceController(DeviceDataRepository repository,
                            DeviceAlertRepository alertRepository,
                            KafkaTemplate<String, String> kafkaTemplate,
                            ObjectMapper objectMapper) {
        this.repository = repository;
        this.alertRepository = alertRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    /**
     * 返回每台设备的最新一条遥测数据。
     */
    @GetMapping("/devices")
    public List<Map<String, Object>> latestDevices() {
        List<DeviceData> all = repository.findAllRawSortedByIdDesc();

        Map<String, DeviceData> latestByDevice = new LinkedHashMap<>();
        for (DeviceData d : all) {
            if (d.getDeviceId() == null || d.getDeviceId().isEmpty()) {
                continue;
            }
            latestByDevice.putIfAbsent(d.getDeviceId(), d);
        }

        return latestByDevice.values().stream()
                .map(d -> {
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("deviceId", d.getDeviceId());
                    m.put("deviceType", d.getDeviceType());
                    m.put("battery", d.getBattery());
                    m.put("xCoordinate", d.getXCoordinate());
                    m.put("yCoordinate", d.getYCoordinate());
                    m.put("status", d.getStatus());
                    return m;
                })
                .collect(Collectors.toList());
    }

    /**
     * 简单的健康检查接口。
     */
    @GetMapping("/health")
    public Map<String, Object> health() {
        return Map.of("status", "UP");
    }

    /**
     * 告警全文检索（GM 在 Console 中查询）。
     * GET /api/alerts/search?keyword=LOW
     *
     * 命中规则：基于 {@code findByAlertMessageContaining}，
     * 对 ES 索引 device_alerts 的 alertMessage 字段做分词匹配。
     */
    @GetMapping("/alerts/search")
    public ResponseEntity<?> searchAlerts(
            @RequestParam(value = "keyword", required = false) String keyword) {
        if (keyword == null || keyword.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of(
                    "ok", false, "error", "keyword 不能为空"));
        }
        try {
            List<DeviceAlert> hits = alertRepository.findByAlertMessageContaining(keyword);

            List<Map<String, Object>> data = hits.stream()
                    .map(a -> {
                        Map<String, Object> m = new LinkedHashMap<>();
                        m.put("id", a.getId());
                        m.put("deviceId", a.getDeviceId());
                        m.put("alertMessage", a.getAlertMessage());
                        m.put("timestamp", a.getTimestamp());
                        return m;
                    })
                    .collect(Collectors.toList());

            // 按时间倒序（ES 默认按 _score，但这里按业务时间更直观）
            data.sort((x, y) -> {
                Long tx = asLong(x.get("timestamp"));
                Long ty = asLong(y.get("timestamp"));
                if (tx == null && ty == null) return 0;
                if (tx == null) return 1;
                if (ty == null) return -1;
                return Long.compare(ty, tx);
            });

            Map<String, Object> resp = new LinkedHashMap<>();
            resp.put("ok", true);
            resp.put("keyword", keyword);
            resp.put("total", data.size());
            resp.put("data", data);
            return ResponseEntity.ok(resp);
        } catch (Exception e) {
            log.error("Alert search failed for keyword='{}': {}", keyword, e.getMessage(), e);
            return ResponseEntity.status(500).body(Map.of(
                    "ok", false, "error", "ES 查询失败: " + e.getMessage()));
        }
    }

    private static Long asLong(Object o) {
        if (o == null) return null;
        if (o instanceof Number n) return n.longValue();
        try {
            return Long.parseLong(String.valueOf(o));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * 任务下发接口（GM → 设备）。
     *
     * 请求体：
     * {
     *   "deviceId": "DRONE-001",
     *   "command":  "GOTO",
     *   "targetX":  12.5,
     *   "targetY":  -3.2
     * }
     *
     * 行为：序列化为 JSON 字符串，投递到 Kafka topic: device-tasks。
     * key = deviceId，保证同一设备的消息顺序消费。
     */
    @PostMapping("/tasks/dispatch")
    public ResponseEntity<Map<String, Object>> dispatchTask(@RequestBody Map<String, Object> body) {
        // 1) 提取 & 校验字段
        String deviceId = asString(body.get("deviceId"));
        String command  = asString(body.get("command"));
        Double targetX  = asDouble(body.get("targetX"));
        Double targetY  = asDouble(body.get("targetY"));

        if (deviceId == null || deviceId.isBlank()) {
            return badRequest("deviceId 不能为空");
        }
        if (command == null || command.isBlank()) {
            return badRequest("command 不能为空");
        }
        if (targetX == null || targetY == null) {
            return badRequest("targetX / targetY 必须是数字");
        }

        // 2) 构造消息体（保持 JSON 顺序，方便设备端解析）
        Map<String, Object> task = new LinkedHashMap<>();
        task.put("deviceId", deviceId);
        task.put("command",  command);
        task.put("targetX",  targetX);
        task.put("targetY",  targetY);
        task.put("issuedAt", System.currentTimeMillis());

        // 序列化：单独捕获 JsonProcessingException
        String payload;
        try {
            payload = objectMapper.writeValueAsString(task);
        } catch (com.fasterxml.jackson.core.JsonProcessingException jpe) {
            log.error("Failed to serialize task {}: {}", task, jpe.getMessage(), jpe);
            return ResponseEntity.status(500).body(Map.of(
                    "ok", false, "error", "JSON 序列化失败: " + jpe.getOriginalMessage()));
        }

        // 发送：捕获 Kafka 相关异常
        try {
            // 同步发送并等待 broker ack，确保 GM 拿到真实结果
            var meta = kafkaTemplate
                    .send(deviceTasksTopic, deviceId, payload)
                    .get(5, TimeUnit.SECONDS);

            log.info("[TASK DISPATCHED] topic={} partition={} offset={} key={} payload={}",
                    meta.getRecordMetadata().topic(),
                    meta.getRecordMetadata().partition(),
                    meta.getRecordMetadata().offset(),
                    deviceId, payload);

            Map<String, Object> resp = new LinkedHashMap<>();
            resp.put("ok", true);
            resp.put("topic", meta.getRecordMetadata().topic());
            resp.put("partition", meta.getRecordMetadata().partition());
            resp.put("offset", meta.getRecordMetadata().offset());
            resp.put("task", task);
            return ResponseEntity.ok(resp);

        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return ResponseEntity.status(500).body(Map.of(
                    "ok", false, "error", "线程被中断: " + ie.getMessage()));
        } catch (ExecutionException | TimeoutException e) {
            log.error("Kafka send failed for task {}: {}", task, e.getMessage(), e);
            return ResponseEntity.status(500).body(Map.of(
                    "ok", false, "error", "Kafka 发送失败: " + e.getMessage()));
        }
    }

    // ------------------------------------------------------------------
    // 工具方法
    // ------------------------------------------------------------------

    private static String asString(Object o) {
        if (o == null) return null;
        return String.valueOf(o).trim();
    }

    private static Double asDouble(Object o) {
        if (o == null) return null;
        if (o instanceof Number n) return n.doubleValue();
        try {
            return Double.parseDouble(String.valueOf(o));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static ResponseEntity<Map<String, Object>> badRequest(String msg) {
        return ResponseEntity.badRequest().body(Map.of("ok", false, "error", msg));
    }
}
