package com.inspection.server.entity;

import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.DateFormat;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

/**
 * Elasticsearch 告警文档。
 *
 * 对应 JSON:
 * {
 *   "id":          "uuid-or-kafka-offset",
 *   "deviceId":    "DRONE-001",
 *   "alertMessage":"LOW BATTERY: Battery at 18%",
 *   "timestamp":   1700000000000
 * }
 */
@Document(indexName = "device_alerts")
public class DeviceAlert {

    @Id
    @Field(type = FieldType.Keyword)
    private String id;

    @Field(type = FieldType.Keyword)
    private String deviceId;

    /** 使用 Text 类型以支持 {@code findByAlertMessageContaining} 全文检索。 */
    @Field(type = FieldType.Text)
    private String alertMessage;

    /**
     * 时间戳（epoch millis）。FieldType.Date 配 epoch_millis 格式，
     * 既支持数值范围查询，又能在 Kibana 里被认成时间字段。
     */
    @Field(type = FieldType.Date, format = DateFormat.epoch_millis)
    private Long timestamp;

    public DeviceAlert() {
    }

    public DeviceAlert(String id, String deviceId, String alertMessage, Long timestamp) {
        this.id = id;
        this.deviceId = deviceId;
        this.alertMessage = alertMessage;
        this.timestamp = timestamp;
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getDeviceId() { return deviceId; }
    public void setDeviceId(String deviceId) { this.deviceId = deviceId; }

    public String getAlertMessage() { return alertMessage; }
    public void setAlertMessage(String alertMessage) { this.alertMessage = alertMessage; }

    public Long getTimestamp() { return timestamp; }
    public void setTimestamp(Long timestamp) { this.timestamp = timestamp; }

    @Override
    public String toString() {
        return "DeviceAlert{id='" + id + "', deviceId='" + deviceId
                + "', alertMessage='" + alertMessage + "', timestamp=" + timestamp + '}';
    }
}
