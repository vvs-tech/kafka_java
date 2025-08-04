package main.java.sbx;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerSsl {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerSsl.class);
    private final Consumer<String, String> consumer;
    private final String topic;

    public KafkaConsumerSsl(String bootstrapServers, String groupId, String topic) {
        this.topic = topic;

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        this.consumer = new KafkaConsumer<>(props);
    }

    public void consume() {
        try {
            consumer.subscribe(Collections.singletonList(topic));

            System.out.println("Consumer started. Waiting for messages...");

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Received message: offset = %d, key = %s, value = %s%n",
                            record.offset(), record.key(), record.value());

                    // Здесь можно добавить обработку сообщения
                }

                // Подтверждаем обработку сообщений (commit offset)
                consumer.commitAsync();
            }
        } catch (Exception e) {
            System.err.println("Error in consumer: " + e.getMessage());
        } finally {
            try {
                consumer.commitSync(); // Финализируем commit перед закрытием
            } finally {
                consumer.close();
                System.out.println("Consumer closed.");
            }
        }
    }

    public static void main(String[] args) {
        String bootstrapServers = "localhost:29092, localhost:39092, localhost:49092";
        String groupId = "test-group-ssl-1";
        String topic = "sandbox";

        KafkaConsumerSsl consumerSsl = new KafkaConsumerSsl(bootstrapServers, groupId, topic);
        consumerSsl.consume();
    }
}
