package com.inspection.server.entity;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

/**
 * MongoDB 文档：设备遥测数据。
 * <p>
 * 对应 JSON:
 * <pre>
 * {
 *   "deviceId":"DRONE-001",
 *   "deviceType":"Drone",
 *   "battery":94.5,
 *   "x_coordinate":0.8,
 *   "y_coordinate":-0.5,
 *   "status":"IDLE"
 * }
 * </pre>
 */
@Document(collection = "device_telemetry")
public class DeviceData {

    /** MongoDB 内部主键（自动生成）。 */
    @Id
    private String id;

    /** 设备唯一标识，例如 DRONE-001。 */
    @Field("deviceId")
    private String deviceId;

    /** 设备类型，例如 Drone / RobotDog。 */
    @Field("deviceType")
    private String deviceType;

    /** 电量百分比，0~100。 */
    @Field("battery")
    private double battery;

    /** X 坐标。 */
    @Field("x_coordinate")
    private double xCoordinate;

    /** Y 坐标。 */
    @Field("y_coordinate")
    private double yCoordinate;

    /** 设备状态：ACTIVE / IDLE / INSPECTING / RETURNING 等。 */
    @Field("status")
    private String status;

    public DeviceData() {
    }

    public DeviceData(String deviceId, String deviceType,
                      double battery, double xCoordinate,
                      double yCoordinate, String status) {
        this.deviceId = deviceId;
        this.deviceType = deviceType;
        this.battery = battery;
        this.xCoordinate = xCoordinate;
        this.yCoordinate = yCoordinate;
        this.status = status;
    }

    // -------- getters / setters --------

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public String getDeviceType() {
        return deviceType;
    }

    public void setDeviceType(String deviceType) {
        this.deviceType = deviceType;
    }

    public double getBattery() {
        return battery;
    }

    public void setBattery(double battery) {
        this.battery = battery;
    }

    public double getXCoordinate() {
        return xCoordinate;
    }

    public void setXCoordinate(double xCoordinate) {
        this.xCoordinate = xCoordinate;
    }

    public double getYCoordinate() {
        return yCoordinate;
    }

    public void setYCoordinate(double yCoordinate) {
        this.yCoordinate = yCoordinate;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "DeviceData{" +
                "id='" + id + '\'' +
                ", deviceId='" + deviceId + '\'' +
                ", deviceType='" + deviceType + '\'' +
                ", battery=" + battery +
                ", xCoordinate=" + xCoordinate +
                ", yCoordinate=" + yCoordinate +
                ", status='" + status + '\'' +
                '}';
    }
}
