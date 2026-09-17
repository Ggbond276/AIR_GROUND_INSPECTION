package com.inspection.simulator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 设备模拟器：
 *   - 主线程：作为 Kafka 生产者，每 3 秒向 device-telemetry 发送两条遥测
 *   - 后台线程：作为 Kafka 消费者，订阅 device-tasks，打印收到的任务
 *
 * 运行：
 *   mvn package
 *   java -jar target/device-simulator-0.0.1-SNAPSHOT.jar
 */
public class DeviceSimulator {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC_TELEMETRY = "device-telemetry";
    private static final String TOPIC_TASKS     = "device-tasks";
    private static final String TOPIC_ALERTS    = "device-alerts";
    private static final String CONSUMER_GROUP  = "device-simulator";
    private static final long SEND_INTERVAL_MS = 3000L;
    private static final double LOW_BATTERY_THRESHOLD = 20.0;

    private static final Random RANDOM = new Random();
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final Device DRONE = new Device(
            "DRONE-001", "Drone", 21.0, 0.0, 0.0, "ACTIVE");

    private static final Device ROBOT_DOG = new Device(
            "ROBOTDOG-001", "RobotDog", 21.0, 10.0, 5.0, "ACTIVE");

    public static void main(String[] args) throws Exception {
        System.out.println("Starting device simulator -> Kafka " + BOOTSTRAP_SERVERS
                + " topics=[" + TOPIC_TELEMETRY + ", " + TOPIC_TASKS + ", " + TOPIC_ALERTS + "]");

        Producer<String, String> producer = new KafkaProducer<>(buildProducerProps());

        AtomicBoolean running = new AtomicBoolean(true);

        // ----- 后台线程：Kafka 消费者 -----
        Thread consumerThread = new Thread(
                new TaskConsumer(running), "kafka-task-consumer");
        consumerThread.setDaemon(true);
        consumerThread.start();

        // ----- JVM 关闭时优雅退出 -----
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down...");
            running.set(false);
            try {
                consumerThread.join(2000);
            } catch (InterruptedException ignored) {
            }
            producer.close(Duration.ofSeconds(5));
        }));

        // ----- 主循环：发遥测 -----
        // 同一个 tick 内避免重复发出 LOW BATTERY 告警
        boolean wasDroneLow  = false;
        boolean wasDogLow    = false;
        try {
            while (running.get()) {
                tick(DRONE);
                tick(ROBOT_DOG);

                send(producer, TOPIC_TELEMETRY, DRONE.deviceId, toJson(DRONE));
                send(producer, TOPIC_TELEMETRY, ROBOT_DOG.deviceId, toJson(ROBOT_DOG));

                // 电量 < 阈值时，下发告警；只在跨越阈值时触发一次（避免每 3 秒狂刷）
                wasDroneLow = maybeSendLowBatteryAlert(producer, DRONE, wasDroneLow);
                wasDogLow   = maybeSendLowBatteryAlert(producer, ROBOT_DOG, wasDogLow);

                try {
                    Thread.sleep(SEND_INTERVAL_MS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        } finally {
            producer.close(Duration.ofSeconds(5));
        }
    }

    /**
     * 如果电量跌破阈值，并且这一次之前还没触发过告警，就向 device-alerts 投递一条 JSON。
     *
     * 消息格式：
     * {"deviceId":"DRONE-001","alertMessage":"LOW BATTERY: Battery at 18%","timestamp":1700000000000}
     */
    private static boolean maybeSendLowBatteryAlert(
            Producer<String, String> producer, Device device, boolean alreadyFired) {
        if (device.battery >= LOW_BATTERY_THRESHOLD) {
            // 电量回升，重置告警状态
            return false;
        }
        if (alreadyFired) {
            return true;
        }
        int pct = (int) Math.floor(device.battery);
        String message = String.format("LOW BATTERY: Battery at %d%%", pct);
        String payload = String.format(
                "{\"deviceId\":\"%s\",\"alertMessage\":\"%s\",\"timestamp\":%d}",
                device.deviceId, message, System.currentTimeMillis());
        System.out.printf("[ALERT] %s battery=%.2f -> sending %s%n",
                device.deviceId, device.battery, message);
        send(producer, TOPIC_ALERTS, device.deviceId, payload);
        return true;
    }

    // ==================================================================
    //  Kafka 消费者（接收任务）
    // ==================================================================
    static class TaskConsumer implements Runnable {

        private final AtomicBoolean running;
        private final KafkaConsumer<String, String> consumer;

        TaskConsumer(AtomicBoolean running) {
            this.running = running;
            this.consumer = new KafkaConsumer<>(buildConsumerProps());
            this.consumer.subscribe(Collections.singletonList(TOPIC_TASKS));
        }

        @Override
        public void run() {
            System.out.println("[CONSUMER] subscribed to topic: " + TOPIC_TASKS);
            try {
                while (running.get()) {
                    // 1 秒 poll 一次，及时响应关闭信号
                    ConsumerRecords<String, String> records =
                            consumer.poll(Duration.ofMillis(1000));

                    for (ConsumerRecord<String, String> record : records) {
                        handle(record);
                    }
                }
            } catch (org.apache.kafka.common.errors.WakeupException we) {
                // 正常关闭路径
            } catch (Exception e) {
                System.err.println("[CONSUMER] error: " + e.getMessage());
                e.printStackTrace();
            } finally {
                try {
                    consumer.close(Duration.ofSeconds(3));
                } catch (Exception ignored) {
                }
                System.out.println("[CONSUMER] closed.");
            }
        }

        private void handle(ConsumerRecord<String, String> record) {
            String payload = record.value();
            System.out.printf("[TASK RAW] partition=%d offset=%d key=%s value=%s%n",
                    record.partition(), record.offset(), record.key(), payload);

            try {
                JsonNode node = MAPPER.readTree(payload);
                String deviceId = textOr(node, "deviceId", record.key());
                String command  = textOr(node, "command",  "?");
                String targetX  = textOr(node, "targetX",  "?");
                String targetY  = textOr(node, "targetY",  "?");

                System.out.printf(
                        "[TASK RECEIVED] Executing task for device: %s, "
                                + "Command: %s, Target: (%s, %s)%n",
                        deviceId, command, targetX, targetY);

                // 这里后续可以接入真正的运动逻辑：移动坐标、切换状态等
            } catch (Exception e) {
                System.err.printf("[TASK PARSE FAIL] value=%s err=%s%n",
                        payload, e.getMessage());
            }
        }

        private static String textOr(JsonNode node, String field, String fallback) {
            JsonNode v = node.get(field);
            return v == null || v.isNull() ? fallback : v.asText();
        }
    }

    // ==================================================================
    //  Producer 配置
    // ==================================================================
    private static Properties buildProducerProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16 * 1024);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "device-simulator-producer");
        return props;
    }

    private static Properties buildConsumerProps() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "device-simulator-consumer");
        return props;
    }

    private static void send(Producer<String, String> producer,
                             String topic, String key, String value) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        try {
            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata meta = future.get();
            System.out.printf("[SENT] topic=%s partition=%d offset=%d key=%s value=%s%n",
                    meta.topic(), meta.partition(), meta.offset(), key, value);
        } catch (Exception e) {
            System.err.printf("[FAILED] key=%s value=%s err=%s%n", key, value, e.getMessage());
        }
    }

    // ==================================================================
    //  设备模拟
    // ==================================================================
    private static void tick(Device device) {
        device.battery = Math.max(0.0, device.battery - RANDOM.nextDouble() * 0.5);
        device.x += (RANDOM.nextDouble() - 0.5) * 2.0;
        device.y += (RANDOM.nextDouble() - 0.5) * 2.0;
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
