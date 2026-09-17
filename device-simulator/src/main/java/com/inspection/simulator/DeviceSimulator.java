package com.inspection.simulator;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;

/**
 * 设备模拟器（Kafka 生产者版）。
 *
 * 行为：
 *   - 模拟两台设备：DRONE-001 和 ROBOTDOG-001
 *   - 每 3 秒为每台设备生成一行 JSON，发送到 Kafka topic: device-telemetry
 *   - 同时在控制台打印发送的内容，便于人工核对
 *
 * 运行：
 *   mvn package
 *   java -jar target/device-simulator-0.0.1-SNAPSHOT.jar
 *
 * 或：
 *   mvn compile exec:java -Dexec.mainClass=com.inspection.simulator.DeviceSimulator
 */
public class DeviceSimulator {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "device-telemetry";
    private static final long SEND_INTERVAL_MS = 3000L;

    private static final Random RANDOM = new Random();

    private static final Device DRONE = new Device(
            "DRONE-001", "Drone", 95.0, 0.0, 0.0, "ACTIVE");

    private static final Device ROBOT_DOG = new Device(
            "ROBOTDOG-001", "RobotDog", 88.0, 10.0, 5.0, "ACTIVE");

    public static void main(String[] args) throws Exception {
        System.out.println("Starting device simulator -> Kafka " + BOOTSTRAP_SERVERS
                + " topic=" + TOPIC);

        Producer<String, String> producer = new KafkaProducer<>(buildProps());

        // JVM 关闭时优雅关闭 producer
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down producer...");
            producer.close(Duration.ofSeconds(5));
        }));

        try {
            while (true) {
                tick(DRONE);
                tick(ROBOT_DOG);

                send(producer, TOPIC, DRONE.deviceId, toJson(DRONE));
                send(producer, TOPIC, ROBOT_DOG.deviceId, toJson(ROBOT_DOG));

                Thread.sleep(SEND_INTERVAL_MS);
            }
        } finally {
            producer.close(Duration.ofSeconds(5));
        }
    }

    // ------------------------------------------------------------------
    // Producer 配置
    // ------------------------------------------------------------------
    private static Properties buildProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 可靠性：等所有 in-sync replica 写完才认为发送成功
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        // 重试 & 幂等：避免重复
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        // 性能参数
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16 * 1024);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        // 客户端标识
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "device-simulator");

        return props;
    }

    /** 同步发送一条消息到 Kafka，并在控制台打印结果。 */
    private static void send(Producer<String, String> producer,
                             String topic, String key, String value) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        try {
            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata meta = future.get(); // 同步等待发送结果
            System.out.printf("[SENT] topic=%s partition=%d offset=%d key=%s value=%s%n",
                    meta.topic(), meta.partition(), meta.offset(), key, value);
        } catch (Exception e) {
            System.err.printf("[FAILED] key=%s value=%s err=%s%n", key, value, e.getMessage());
        }
    }

    // ------------------------------------------------------------------
    // 设备模拟
    // ------------------------------------------------------------------
    private static void tick(Device device) {
        // 电量缓慢下降
        device.battery = Math.max(0.0, device.battery - RANDOM.nextDouble() * 0.5);
        // 坐标轻微漂移
        device.x += (RANDOM.nextDouble() - 0.5) * 2.0;
        device.y += (RANDOM.nextDouble() - 0.5) * 2.0;
        // 状态切换
        String[] statuses = {"ACTIVE", "IDLE", "INSPECTING", "RETURNING"};
        device.status = statuses[RANDOM.nextInt(statuses.length)];
    }

    private static String toJson(Device device) {
        return String.format(
                "{\"deviceId\":\"%s\",\"deviceType\":\"%s\","
                        + "\"battery\":%.2f,\"x_coordinate\":%.2f,"
                        + "\"y_coordinate\":%.2f,\"status\":\"%s\"}",
                device.deviceId, device.deviceType, device.battery,
                device.x, device.y, device.status);
    }

    /** 简单的设备数据载体。 */
    static class Device {
        String deviceId;
        String deviceType;
        double battery;
        double x;
        double y;
        String status;

        Device(String deviceId, String deviceType,
               double battery, double x, double y, String status) {
            this.deviceId = deviceId;
            this.deviceType = deviceType;
            this.battery = battery;
            this.x = x;
            this.y = y;
            this.status = status;
        }
    }
}
