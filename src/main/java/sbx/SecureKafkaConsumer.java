package main.java.sbx;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class SecureKafkaConsumer {
    private static final Logger log = LoggerFactory.getLogger(SecureKafkaConsumer.class);
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Consumer<String, String> consumer;
    private final String topic;

    public SecureKafkaConsumer(String bootstrapServers, String topic, Properties securityProps) {
        this.topic = topic;

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-ssl-2");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // Добавляем SSL/SASL настройки
        props.putAll(securityProps);

        this.consumer = new KafkaConsumer<>(props);
    }

    public void start() {
        try {
            consumer.subscribe(Collections.singletonList(topic));
            log.info("Subscribed to topic: {}", topic);

            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    log.info(
                            "Received message: key={}, value={}, partition={}, offset={}",
                            record.key(), record.value(), record.partition(), record.offset()
                    );
                }

                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }
        } catch (Exception e) {
            log.error("Consumer error", e);
        } finally {
            shutdown();
        }
    }

    public void shutdown() {
        running.set(false);
        consumer.wakeup(); // Прерываем poll()
        consumer.close();
        log.info("Consumer shutdown complete");
    }

    public static void main(String[] args) {
        // Настройки SSL
        Properties securityProps = new Properties();

        // 1. Включение SSL
        securityProps.put("security.protocol", "SSL");

        // 2. Пути к SSL-файлам (если требуется)
        securityProps.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "kafka.truststore.jks");
        securityProps.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "wAUyWEpL<>tWU9VxcAipcZP");

        // 3. Если требуется двусторонняя аутентификация (client auth)
        securityProps.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "kafka.keystore.jks");
        securityProps.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "wAUyWEpL<>tWU9VxcAipcZP");
//        securityProps.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, "key-password");

        // 4. Доп. настройки (если нужно)
        securityProps.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, ""); // Отключает hostname verification

        SecureKafkaConsumer consumer = new SecureKafkaConsumer(
                "dvdlsm-00kfp001.innodev.local:9092,dvdlsm-00kfp002.innodev.local:9092,dvdlsm-00kfp003.innodev.local:9092", // SSL-порт обычно 9093
                "test_topic",
                securityProps
        );

        // Graceful shutdown при Ctrl+C
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::shutdown));

        consumer.start();
    }
}




