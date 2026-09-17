package com.inspection.server.controller;

import com.inspection.server.entity.DeviceData;
import com.inspection.server.repository.DeviceDataRepository;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * GM Console / 设备状态查询接口。
 */
@RestController
@RequestMapping("/api")
public class DeviceController {

    private final DeviceDataRepository repository;

    public DeviceController(DeviceDataRepository repository) {
        this.repository = repository;
    }

    /**
     * 返回每台设备的最新一条遥测数据。
     *
     * 示例响应：
     * [
     *   {"deviceId":"DRONE-001",     "deviceType":"Drone",    "battery":94.5, "xCoordinate":0.8, "yCoordinate":-0.5, "status":"IDLE"},
     *   {"deviceId":"ROBOTDOG-001",  "deviceType":"RobotDog", "battery":87.2, "xCoordinate":9.8, "yCoordinate":4.4,  "status":"ACTIVE"}
     * ]
     */
    @GetMapping("/devices")
    public List<Map<String, Object>> latestDevices() {
        // 1) 取所有记录，按 id 倒序（最新在前）
        List<DeviceData> all = repository.findAllRawSortedByIdDesc();

        // 2) 按 deviceId 去重，只保留第一条（即最新一条）
        Map<String, DeviceData> latestByDevice = new LinkedHashMap<>();
        for (DeviceData d : all) {
            // 同一 deviceId 可能为 null / 空，做保护
            if (d.getDeviceId() == null || d.getDeviceId().isEmpty()) {
                continue;
            }
            latestByDevice.putIfAbsent(d.getDeviceId(), d);
        }

        // 3) 转成前端友好的 Map（避免暴露 _id 等字段）
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
     * 简单的健康检查接口，方便前端探测后端是否就绪。
     */
    @GetMapping("/health")
    public Map<String, Object> health() {
        return Map.of("status", "UP");
    }
}
