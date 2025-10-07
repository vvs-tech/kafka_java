package main.java.sbx;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicListing;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.Set;

public class KafkaTopicLister {

    private final String bootstrapServers;

    public KafkaTopicLister(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public void listTopics() {

        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(AdminClientConfig.CLIENT_ID_CONFIG, "kafka-topic-lister");
        properties.put("security.protocol", "SSL");
        properties.put("ssl.truststore.location", "kafka.truststore.jks");
        properties.put("ssl.truststore.password", "truststore-password");
        properties.put("ssl.keystore.location", "kafka.keystore.jks");
        properties.put("ssl.keystore.password", "wAUyWEpL<>tWU9VxcAipcZP");

        try (AdminClient adminClient = AdminClient.create(properties)) {

            ListTopicsResult topicsResult = adminClient.listTopics();
            Set<String> topicNames = topicsResult.names().get();

            System.out.println("Список топиков Kafka:");
            System.out.println("======================");

            if (topicNames.isEmpty()) {
                System.out.println("Топики не найдены");
            } else {
                for (String topicName : topicNames) {
                    System.out.println("- " + topicName);
                }
            }

        } catch (InterruptedException | ExecutionException e) {
            System.err.println("Ошибка при получении списка топиков: " + e.getMessage());
        }
    }


    public static void main(String[] args) {

//        String bootstrapServers = "localhost:29092, localhost:39092, localhost:49092";
        String bootstrapServers = "stpimc-00kfp002.innodev.local:9092,stpimc-00kfp003.innodev.local:9092,stpimc-00kfp004.innodev.local:9092";

        KafkaTopicLister lister = new KafkaTopicLister(bootstrapServers);
        lister.listTopics();
    }
}