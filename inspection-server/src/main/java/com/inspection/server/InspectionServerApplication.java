package com.inspection.server;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 空地一体巡检平台 —— 主服务端（World Server）入口。
 *
 * 职责：
 *   1. 监听 Kafka 的 device-telemetry 主题；
 *   2. 把遥测数据写入 MongoDB；
 *   3. 后续会扩展 REST API / WebSocket / 调度等。
 */
@SpringBootApplication
public class InspectionServerApplication {

    public static void main(String[] args) {
        SpringApplication.run(InspectionServerApplication.class, args);
    }
}
